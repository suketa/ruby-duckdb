# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class AppendArrowTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
      @conn.query('CREATE TABLE source (id INTEGER, name VARCHAR)')
      @conn.query("INSERT INTO source VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Cathy')")
      @conn.query('CREATE TABLE dest (id INTEGER, name VARCHAR)')
    end

    def teardown
      @conn.disconnect
      @db.close
    end

    def test_append_arrow_loads_an_arrow_producer_into_an_existing_table
      producer = @conn.query('SELECT * FROM source ORDER BY id')

      rows = @conn.append_arrow('dest', producer)

      assert_equal 3, rows
      assert_equal [[1, 'Alice'], [2, 'Bob'], [3, 'Cathy']],
                   @conn.query('SELECT * FROM dest ORDER BY id').to_a
    end

    def test_append_arrow_raises_type_error_for_non_producer
      assert_raises(TypeError) { @conn.append_arrow('dest', 'not a producer') }
    end

    def test_append_arrow_casts_compatible_column_types
      @conn.query('CREATE TABLE widened (id BIGINT, name VARCHAR)') # source id is INTEGER
      producer = @conn.query('SELECT * FROM source ORDER BY id')

      rows = @conn.append_arrow('widened', producer)

      assert_equal 3, rows
      assert_equal [[1, 'Alice'], [2, 'Bob'], [3, 'Cathy']],
                   @conn.query('SELECT * FROM widened ORDER BY id').to_a
    end

    def test_append_arrow_raises_on_incompatible_column_type
      @conn.query('CREATE TABLE bad (id INTEGER, name INTEGER)') # name 'Alice' cannot cast to INTEGER
      producer = @conn.query('SELECT * FROM source')

      assert_raises(DuckDB::Error) { @conn.append_arrow('bad', producer) }
    end

    def test_append_arrow_appends_nothing_for_an_empty_producer
      producer = @conn.query('SELECT * FROM source WHERE id > 100')

      rows = @conn.append_arrow('dest', producer)

      assert_equal 0, rows
      assert_equal 0, @conn.query('SELECT COUNT(*) FROM dest').to_a.first.first
    end

    def test_append_arrow_consumes_a_result_producer_only_once
      producer = @conn.query('SELECT * FROM source')
      @conn.append_arrow('dest', producer)

      assert_raises(DuckDB::Error) { @conn.append_arrow('dest', producer) }
    end

    def test_append_arrow_is_gc_safe
      @conn.query('CREATE TABLE acc (id INTEGER, name VARCHAR)')

      5.times do
        @conn.append_arrow('acc', @conn.query('SELECT * FROM source'))
        GC.start
      end
      GC.start

      assert_equal 15, @conn.query('SELECT COUNT(*) FROM acc').to_a.first.first
    end

    def test_append_arrow_streams_multiple_chunks
      @conn.query('CREATE TABLE big_src (id INTEGER)')
      @conn.query('INSERT INTO big_src SELECT * FROM range(5000)') # > one 2048-row chunk
      @conn.query('CREATE TABLE big_dest (id INTEGER)')
      producer = @conn.query('SELECT * FROM big_src')

      rows = @conn.append_arrow('big_dest', producer)

      assert_equal 5000, rows
      assert_equal 5000, @conn.query('SELECT COUNT(*) FROM big_dest').to_a.first.first
    end
  end
end
