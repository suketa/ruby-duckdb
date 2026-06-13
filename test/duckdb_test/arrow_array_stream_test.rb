# frozen_string_literal: true

require 'test_helper'
require 'fiddle'

module DuckDBTest
  class ArrowArrayStreamTest < Minitest::Test
    # Arrow C Data Interface struct layouts (64-bit):
    #   struct ArrowArrayStream: get_schema@0, get_next@8, get_last_error@16,
    #                            release@24, private_data@32
    #   struct ArrowSchema: format@0, name@8, metadata@16, flags@24,
    #                       n_children@32, children@40, dictionary@48,
    #                       release@56, private_data@64 (size 72)
    ARROW_SCHEMA_SIZE = 72
    #   struct ArrowArray: length@0, null_count@8, offset@16, n_buffers@24,
    #                      n_children@32, buffers@40, children@48,
    #                      dictionary@56, release@64, private_data@72 (size 80)
    ARROW_ARRAY_SIZE = 80

    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
      @conn.query('CREATE TABLE users (id INTEGER, name VARCHAR)')
      @conn.query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Cathy')")
    end

    def teardown
      @conn.disconnect
      @db.close
    end

    def test_arrow_c_stream_returns_arrow_array_stream # rubocop:disable Minitest/MultipleAssertions
      result = @conn.query('SELECT * FROM users')
      stream = result.arrow_c_stream

      assert_instance_of DuckDB::ArrowArrayStream, stream
      assert_kind_of Integer, stream.to_i
      refute_equal 0, stream.to_i
      assert_same stream, stream.arrow_c_stream
    end

    def test_get_schema_describes_result_columns # rubocop:disable Minitest/MultipleAssertions
      stream = @conn.query('SELECT * FROM users').arrow_c_stream

      schema = Fiddle::Pointer.malloc(ARROW_SCHEMA_SIZE, Fiddle::RUBY_FREE)
      ret = call_stream_fn(stream.to_i, 0, schema)

      assert_equal 0, ret, "get_schema failed: #{stream_last_error(stream.to_i)}"
      assert_equal '+s', read_cstr(schema, 0) # top-level struct type
      assert_equal 2, read_int64(schema, 32)  # n_children

      children = read_ptr(schema, 40)
      child0 = Fiddle::Pointer.new(read_ptr(Fiddle::Pointer.new(children), 0))
      child1 = Fiddle::Pointer.new(read_ptr(Fiddle::Pointer.new(children), 8))

      assert_equal 'id', read_cstr(child0, 8)
      assert_equal 'i', read_cstr(child0, 0)   # INTEGER -> int32
      assert_equal 'name', read_cstr(child1, 8)
      assert_equal 'u', read_cstr(child1, 0)   # VARCHAR -> utf8
    ensure
      release_arrow_struct(schema, 56) if schema
    end

    def test_get_next_streams_chunks_then_signals_end_of_stream # rubocop:disable Minitest/MultipleAssertions
      stream = @conn.query('SELECT * FROM users ORDER BY id').arrow_c_stream
      array = Fiddle::Pointer.malloc(ARROW_ARRAY_SIZE, Fiddle::RUBY_FREE)

      ret = call_stream_fn(stream.to_i, 8, array)

      assert_equal 0, ret, "get_next failed: #{stream_last_error(stream.to_i)}"
      refute_predicate read_ptr(array, 64), :zero?, 'expected a live chunk (release != NULL)'
      assert_equal 3, read_int64(array, 0)  # rows in chunk
      assert_equal 2, read_int64(array, 32) # one child per column

      id_column = Fiddle::Pointer.new(read_ptr(Fiddle::Pointer.new(read_ptr(array, 48)), 0))
      id_data = Fiddle::Pointer.new(read_ptr(Fiddle::Pointer.new(read_ptr(id_column, 40)), 8))

      assert_equal [1, 2, 3], id_data[0, 12].unpack('l3')

      release_arrow_struct(array, 64)
      ret = call_stream_fn(stream.to_i, 8, array)

      assert_equal 0, ret
      assert_predicate read_ptr(array, 64), :zero?, 'expected end-of-stream (release == NULL)'
    end

    def test_arrow_c_stream_raises_when_result_is_already_exported
      result = @conn.query('SELECT * FROM users')
      result.arrow_c_stream

      e = assert_raises(DuckDB::Error) { result.arrow_c_stream }

      assert_match(/already exported/, e.message)
    end

    def test_arrow_array_stream_cannot_be_instantiated_directly
      assert_raises(DuckDB::Error) { DuckDB::ArrowArrayStream.new }
    end

    def test_get_next_signals_end_of_stream_for_empty_result
      stream = @conn.query('SELECT * FROM users WHERE id > 100').arrow_c_stream
      array = Fiddle::Pointer.malloc(ARROW_ARRAY_SIZE, Fiddle::RUBY_FREE)

      ret = call_stream_fn(stream.to_i, 8, array)

      assert_equal 0, ret
      assert_predicate read_ptr(array, 64), :zero?, 'expected immediate end-of-stream'
    end

    def test_stream_keeps_result_alive_across_gc
      stream = @conn.query('SELECT * FROM users ORDER BY id').arrow_c_stream
      GC.start
      array = Fiddle::Pointer.malloc(ARROW_ARRAY_SIZE, Fiddle::RUBY_FREE)

      ret = call_stream_fn(stream.to_i, 8, array)

      assert_equal 0, ret, "get_next failed: #{stream_last_error(stream.to_i)}"
      assert_equal 3, read_int64(array, 0)
      release_arrow_struct(array, 64)
    end

    def test_stream_release_callback_marks_stream_released
      stream = @conn.query('SELECT * FROM users').arrow_c_stream
      ptr = Fiddle::Pointer.new(stream.to_i)

      release_arrow_struct(ptr, 24)

      assert_predicate read_ptr(ptr, 24), :zero?, 'expected release pointer to be NULL after release'
    end

    private

    # Calls a function pointer stored at +offset+ in struct ArrowArrayStream.
    def call_stream_fn(stream_address, offset, out_ptr)
      fn = read_ptr(Fiddle::Pointer.new(stream_address), offset)
      Fiddle::Function.new(fn, [Fiddle::TYPE_VOIDP, Fiddle::TYPE_VOIDP], Fiddle::TYPE_INT)
                      .call(stream_address, out_ptr)
    end

    def stream_last_error(stream_address)
      fn = read_ptr(Fiddle::Pointer.new(stream_address), 16)
      err = Fiddle::Function.new(fn, [Fiddle::TYPE_VOIDP], Fiddle::TYPE_VOIDP)
                            .call(stream_address)
      err.null? ? nil : err.to_s
    end

    def read_ptr(ptr, offset)
      ptr[offset, 8].unpack1('Q')
    end

    def read_int64(ptr, offset)
      ptr[offset, 8].unpack1('q')
    end

    def read_cstr(ptr, offset)
      Fiddle::Pointer.new(read_ptr(ptr, offset)).to_s
    end

    # Invokes the release callback stored at +offset+ of an ArrowSchema or
    # ArrowArray struct, unless the struct is already released (release == NULL).
    def release_arrow_struct(ptr, offset)
      fn = read_ptr(ptr, offset)
      return if fn.zero?

      Fiddle::Function.new(fn, [Fiddle::TYPE_VOIDP], Fiddle::TYPE_VOID).call(ptr)
    end
  end
end
