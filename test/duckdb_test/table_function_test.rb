# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class TableFunctionTest < Minitest::Test
    # Test 1: Create using new
    def test_new
      tf = DuckDB::TableFunction.new

      assert_instance_of DuckDB::TableFunction, tf
    end

    # Test: Create function with set_value (high-level API)
    # rubocop:disable Minitest/MultipleAssertions
    def test_create_with_set_value
      db = DuckDB::Database.open
      conn = db.connect

      called = 0

      tf = DuckDB::TableFunction.create(
        name: 'test_set_value',
        columns: { 'id' => DuckDB::LogicalType::BIGINT, 'name' => DuckDB::LogicalType::VARCHAR }
      ) do |_func_info, output|
        called += 1
        if called > 1
          0 # Return 0 rows (done)
        else
          # Use high-level set_value API
          output.set_value(0, 0, 1)
          output.set_value(1, 0, 'Alice')

          output.set_value(0, 1, 2)
          output.set_value(1, 1, 'Bob')

          2 # Return 2 rows
        end
      end

      conn.register_table_function(tf)
      result = conn.query('SELECT * FROM test_set_value()')
      rows = result.each.to_a

      assert_equal 2, rows.size
      assert_equal 1, rows[0][0]
      assert_equal 'Alice', rows[0][1]
      assert_equal 2, rows[1][0]
      assert_equal 'Bob', rows[1][1]

      conn.disconnect
      db.close
    end
    # rubocop:enable Minitest/MultipleAssertions

    # Test: Create requires name
    def test_create_requires_name
      assert_raises(ArgumentError) do
        DuckDB::TableFunction.create(
          columns: { 'value' => DuckDB::LogicalType::BIGINT }
        ) do |_func_info, _output|
          0 # Return 0 rows (done)
        end
      end
    end

    # Test: Create requires columns
    def test_create_requires_columns
      assert_raises(ArgumentError) do
        DuckDB::TableFunction.create(
          name: 'test'
        ) do |_func_info, _output|
          0 # Return 0 rows (done)
        end
      end
    end

    # Test: Create requires block
    def test_create_requires_block
      assert_raises(ArgumentError) do
        DuckDB::TableFunction.create(
          name: 'test',
          columns: { 'value' => DuckDB::LogicalType::BIGINT }
        )
      end
    end

    def test_gc_compaction_safety # rubocop:disable Minitest/MultipleAssertions
      skip 'GC.compact not available' unless GC.respond_to?(:compact)

      db = DuckDB::Database.open
      conn = db.connect

      # Capture local variable in callbacks
      row_multiplier = 2
      done = false

      tf = DuckDB::TableFunction.new
      tf.name = 'test_gc_compact'

      # Bind callback
      tf.bind do |bind_info|
        bind_info.add_result_column('id', DuckDB::LogicalType::BIGINT)
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      # Init callback
      tf.init do |_init_info|
        done = false # Reset state
      end

      # Execute callback that captures local variable
      tf.execute do |_func_info, output|
        if done
          output.size = 0
        else
          output.set_value(0, 0, 1)
          output.set_value(1, 0, 10 * row_multiplier)
          output.set_value(0, 1, 2)
          output.set_value(1, 1, 20 * row_multiplier)
          output.size = 2
          done = true
        end
      end

      conn.register_table_function(tf)

      # Force GC compaction
      GC.compact

      # Query multiple times
      3.times do
        result = conn.query('SELECT * FROM test_gc_compact()')
        rows = result.each.to_a

        assert_equal 2, rows.size
        assert_equal 1, rows[0][0]
        assert_equal 20, rows[0][1], 'Execute callback failed after GC compaction'
        done = false # Reset for next query
      end

      # Force another compaction
      GC.compact
      result = conn.query('SELECT * FROM test_gc_compact()')
      rows = result.each.to_a

      assert_equal 2, rows.size

      conn.disconnect
      db.close
    end

    def test_symbol_columns
      db = DuckDB::Database.open
      conn = db.connect

      # Capture local variable in callbacks
      row_multiplier = 2
      done = false

      tf = DuckDB::TableFunction.new
      tf.name = 'test_symbol_columns'

      # Bind callback
      tf.bind do |bind_info|
        bind_info.add_result_column(:id, :bigint)
        bind_info.add_result_column(:value, :bigint)
      end

      # Init callback
      tf.init do |_init_info|
        done = false # Reset state
      end

      # Execute callback that captures local variable
      tf.execute do |_func_info, output|
        if done
          output.size = 0
        else
          output.set_value(0, 0, 1)
          output.set_value(1, 0, 10 * row_multiplier)
          output.set_value(0, 1, 2)
          output.set_value(1, 1, 20 * row_multiplier)
          output.size = 2
          done = true
        end
      end

      conn.register_table_function(tf)

      result = conn.query('SELECT * FROM test_symbol_columns()')
      rows = result.each.to_a

      assert_equal 2, rows.size
      assert_equal 1, rows[0][0]
      assert_equal 20, rows[0][1], 'Execute callback failed after GC compaction'

      conn.disconnect
      db.close
    end

    # Per-worker proxy: exercises the local_init -> proxy -> destroy lifecycle
    # under real multi-threaded execution (SET threads=4) and asserts the
    # proxy path actually fired: execute callbacks must run on more than two
    # distinct Ruby threads. Without per-worker proxies that count can never
    # exceed two (the calling thread plus the single global executor), so this
    # fails on the old implementation. Simultaneity assertions (max
    # concurrency) are avoided as scheduler-dependent; sample/issue1136.rb
    # demonstrates the throughput win for the scalar twin of this mechanism.
    # Requires DuckDB >= 1.5.0 (duckdb_table_function_set_local_init).
    def test_execute_runs_on_per_worker_proxy_threads
      if ::DuckDBTest.duckdb_library_version < Gem::Version.new('1.5.0')
        skip 'per-worker proxy requires DuckDB >= 1.5.0'
      end

      chunks = 64
      rows_per_chunk = 100
      remaining = chunks
      mutex = Mutex.new
      threads_seen = {}

      db = DuckDB::Database.open
      conn = db.connect
      conn.execute('SET threads=4')

      tf = DuckDB::TableFunction.new
      tf.name = 'parallel_emitter'
      tf.bind do |bind_info|
        bind_info.add_result_column('v', DuckDB::LogicalType::BIGINT)
        # Tell the planner there is real work so it distributes across workers.
        bind_info.set_cardinality(chunks * rows_per_chunk, false)
      end
      tf.init do |init_info|
        # Without this DuckDB assigns a single worker and the proxy never fires.
        init_info.max_threads = 4
      end
      tf.execute do |_info, output|
        threads_seen[Thread.current] = true
        has_work = mutex.synchronize do
          next false if remaining.zero?

          remaining -= 1
          true
        end

        unless has_work
          output.size = 0
          next
        end

        rows_per_chunk.times { |i| output.set_value(0, i, 1) }
        output.size = rows_per_chunk
        sleep 0.001 # release the GVL so workers can overlap
      end

      conn.register_table_function(tf)
      result = conn.query('SELECT COUNT(*), SUM(v) FROM parallel_emitter()').each.to_a

      assert_equal [chunks * rows_per_chunk, chunks * rows_per_chunk], result.first
      assert_operator threads_seen.size, :>, 2,
                      'expected callbacks on per-worker proxy threads, not just caller + global executor'

      conn.disconnect
      db.close
    end

    private

    def setup_incomplete_function
      database = DuckDB::Database.open
      conn = database.connect
      table_function = DuckDB::TableFunction.new
      table_function.name = 'incomplete_function'
      table_function.add_parameter(DuckDB::LogicalType::BIGINT)
      [database, conn, table_function]
    end

    def cleanup_function(_table_function, conn, database)
      conn.disconnect
      database.close
    end
  end
end
