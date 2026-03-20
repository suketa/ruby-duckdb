# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class TableFunctionTest < Minitest::Test
    def setup
      skip 'TableFunction tests with Ruby callbacks hang on Windows' if Gem.win_platform?
    end

    # Test 1: Create using new
    def test_new
      tf = DuckDB::TableFunction.new

      assert_instance_of DuckDB::TableFunction, tf
    end

    # Test: Create function with set_value (high-level API)
    # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions
    def test_create_with_set_value
      db = DuckDB::Database.open
      conn = db.connect
      conn.query('SET threads=1')

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
    # rubocop:enable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions

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

    # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions
    def test_gc_compaction_safety
      skip 'GC.compact not available' unless GC.respond_to?(:compact)

      db = DuckDB::Database.open
      conn = db.connect
      conn.query('SET threads=1')

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
    # rubocop:enable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions

    # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
    def test_symbol_columns
      db = DuckDB::Database.open
      conn = db.connect
      conn.query('SET threads=1')

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
    # rubocop:enable Metrics/AbcSize, Metrics/MethodLength

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
