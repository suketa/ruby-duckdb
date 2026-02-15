# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class TableFunctionTest < Minitest::Test
    # Test 1: Create using new
    def test_new
      tf = DuckDB::TableFunction.new

      assert_instance_of DuckDB::TableFunction, tf
    end

    # Test 2: Set name
    def test_set_name
      tf = DuckDB::TableFunction.new
      tf.name = 'my_function'
      tf
    end

    # Test 3: Add positional parameter
    def test_add_parameter
      tf = DuckDB::TableFunction.new
      tf.name = 'my_function'
      tf.add_parameter(DuckDB::LogicalType::BIGINT)
    end

    # Test 4: Add named parameter
    def test_add_named_parameter
      tf = DuckDB::TableFunction.new
      tf.name = 'my_function'
      tf.add_named_parameter('limit', DuckDB::LogicalType::BIGINT)
    end

    # Test 5: Register without callbacks (should fail gracefully)
    # TODO: Enable this test once database connection issue is resolved
    def _test_register_without_callbacks
      database, conn, table_function = setup_incomplete_function

      # Should fail because no bind/init/execute callbacks set
      assert_raises(DuckDB::Error) do
        conn.register_table_function(table_function)
      end

      cleanup_function(table_function, conn, database)
    end

    # ========================================
    # Tests for TableFunction.create (TDD)
    # ========================================

    # Test: Create with minimal parameters
    def test_create_minimal
      tf = DuckDB::TableFunction.create(
        name: 'test_range',
        parameters: [DuckDB::LogicalType::BIGINT],
        columns: { 'value' => DuckDB::LogicalType::BIGINT }
      ) do |_func_info, _output|
        0 # Return 0 rows (done)
      end

      assert_instance_of DuckDB::TableFunction, tf
    end

    # Test: Create and use simple range function
    # rubocop:disable Metrics/MethodLength
    def test_create_range_function
      db = DuckDB::Database.open
      conn = db.connect
      conn.query('SET threads=1')

      # Simple test: create a function that returns empty result
      tf = DuckDB::TableFunction.create(
        name: 'test_range',
        parameters: [DuckDB::LogicalType::BIGINT],
        columns: { 'value' => DuckDB::LogicalType::BIGINT }
      ) do |_func_info, _output|
        0 # Return 0 rows (done)
      end

      conn.register_table_function(tf)
      result = conn.query('SELECT * FROM test_range(5)')
      rows = result.each.to_a

      assert_equal 0, rows.size

      conn.disconnect
      db.close
    end
    # rubocop:enable Metrics/MethodLength

    # Test: Create function that returns data
    # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
    def test_create_with_data_output
      db = DuckDB::Database.open
      conn = db.connect
      conn.query('SET threads=1')

      called = 0

      tf = DuckDB::TableFunction.create(
        name: 'test_data',
        columns: { 'value' => DuckDB::LogicalType::BIGINT }
      ) do |_func_info, output|
        called += 1
        if called > 1
          0  # Return 0 rows (done)
        else
          vec = output.get_vector(0)
          data = vec.get_data
          (0...3).each do |i|
            DuckDB::MemoryHelper.write_bigint(data, i, i * 10)
          end

          3  # Return 3 rows
        end
      end

      conn.register_table_function(tf)
      result = conn.query('SELECT * FROM test_data()')
      rows = result.each.to_a

      assert_equal 3, rows.size
      assert_equal([0, 10, 20], rows.map { |r| r[0] })

      conn.disconnect
      db.close
    end
    # rubocop:enable Metrics/AbcSize, Metrics/MethodLength

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

    # Test: Create with named parameters
    def test_create_with_named_parameters
      tf = DuckDB::TableFunction.create(
        name: 'test_named',
        parameters: { 'limit' => DuckDB::LogicalType::BIGINT },
        columns: { 'value' => DuckDB::LogicalType::BIGINT }
      ) do |_func_info, _output|
        0 # Return 0 rows (done)
      end

      assert_instance_of DuckDB::TableFunction, tf
    end

    # Test: Create with multiple columns
    # rubocop:disable Metrics/MethodLength
    def test_create_with_multiple_columns
      tf = DuckDB::TableFunction.create(
        name: 'test_multi',
        parameters: [DuckDB::LogicalType::BIGINT],
        columns: {
          'id' => DuckDB::LogicalType::BIGINT,
          'name' => DuckDB::LogicalType::VARCHAR,
          'value' => DuckDB::LogicalType::DOUBLE
        }
      ) do |_func_info, _output|
        0 # Return 0 rows (done)
      end

      assert_instance_of DuckDB::TableFunction, tf
    end
    # rubocop:enable Metrics/MethodLength

    # Test: Create without parameters
    def test_create_without_parameters
      tf = DuckDB::TableFunction.create(
        name: 'test_no_params',
        columns: { 'value' => DuckDB::LogicalType::BIGINT }
      ) do |_func_info, _output|
        0 # Return 0 rows (done)
      end

      assert_instance_of DuckDB::TableFunction, tf
    end

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
      skip 'GC.compact hangs on Windows in parallel test execution' if Gem.win_platform?

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
