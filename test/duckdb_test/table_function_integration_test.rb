# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class TableFunctionIntegrationTest < Minitest::Test
    def setup
      @database = DuckDB::Database.open
      @connection = @database.connect
      @connection.execute('SET threads=1') # Required for Ruby callbacks
    end

    def teardown
      @connection.disconnect
      @database.close
    end

    # Test 1: Simple table function returning data
    # rubocop:disable Minitest/MultipleAssertions
    def test_simple_table_function
      table_function = create_simple_function

      @connection.register_table_function(table_function)
      result = @connection.query('SELECT * FROM simple_function()')

      rows = result.each.to_a

      assert_equal 3, rows.count
      assert_equal [1, 'Alice'], rows[0]
      assert_equal [2, 'Bob'], rows[1]
      assert_equal [3, 'Charlie'], rows[2]
    end
    # rubocop:enable Minitest/MultipleAssertions

    # Test 2: Table function with parameters
    # rubocop:disable Minitest/MultipleAssertions
    def test_table_function_with_parameters
      table_function = create_parameterized_function

      @connection.register_table_function(table_function)
      result = @connection.query("SELECT * FROM repeat_string('hello', 3)")

      rows = result.each.to_a

      assert_equal 3, rows.count
      assert_equal ['test'], rows[0]
      assert_equal ['test'], rows[1]
      assert_equal ['test'], rows[2]
    end
    # rubocop:enable Minitest/MultipleAssertions

    # Test 3: Named TIMESTAMP_S parameter is converted to Ruby Time via rbduckdb_duckdb_value_to_ruby
    def test_get_named_parameter_returns_time_for_timestamp_s # rubocop:disable Minitest/MultipleAssertions, Metrics/AbcSize, Metrics/MethodLength
      captured = nil
      tf = DuckDB::TableFunction.new
      tf.name = 'test_ts_s_named'
      tf.add_named_parameter('ts', DuckDB::LogicalType.resolve(:timestamp_s))
      tf.bind do |bind_info|
        captured = bind_info.get_named_parameter('ts')
        bind_info.add_result_column('v', DuckDB::LogicalType::BIGINT)
      end
      tf.init { |_i| nil }
      tf.execute { |_f, output| output.size = 0 }

      @connection.register_table_function(tf)
      @connection.execute("SELECT * FROM test_ts_s_named(ts = '2025-03-15 08:30:45'::TIMESTAMP_S)")

      assert_instance_of Time, captured
      assert_equal 2025, captured.year
      assert_equal 3,    captured.month
      assert_equal 15,   captured.day
      assert_equal 8,    captured.hour
      assert_equal 30,   captured.min
      assert_equal 45,   captured.sec
    end

    private

    # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
    def create_simple_function
      done = false # Track state with closure variable

      table_function = DuckDB::TableFunction.new
      table_function.name = 'simple_function'

      table_function.bind do |bind_info|
        bind_info.add_result_column('id', DuckDB::LogicalType::BIGINT)
        bind_info.add_result_column('name', DuckDB::LogicalType::VARCHAR)
      end

      table_function.init { |_init_info| done = false } # Reset state

      table_function.execute do |_func_info, output|
        if done
          # Already returned all rows
          output.size = 0
        else
          # Get vectors for both columns
          id_vector = output.get_vector(0)
          name_vector = output.get_vector(1)

          # Write data to id column (BIGINT)
          id_data = id_vector.get_data
          DuckDB::MemoryHelper.write_bigint(id_data, 0, 1)
          DuckDB::MemoryHelper.write_bigint(id_data, 1, 2)
          DuckDB::MemoryHelper.write_bigint(id_data, 2, 3)

          # Write data to name column (VARCHAR)
          name_vector.assign_string_element(0, 'Alice')
          name_vector.assign_string_element(1, 'Bob')
          name_vector.assign_string_element(2, 'Charlie')

          # Set the number of rows
          output.size = 3
          done = true
        end
      end

      table_function
    end

    def create_parameterized_function
      done = false # Track state

      table_function = DuckDB::TableFunction.new
      table_function.name = 'repeat_string'
      table_function.add_parameter(DuckDB::LogicalType::VARCHAR)
      table_function.add_parameter(DuckDB::LogicalType::BIGINT)

      table_function.bind do |bind_info|
        # Just define output schema - we'll read parameters in execute
        bind_info.add_result_column('value', DuckDB::LogicalType::VARCHAR)
      end

      table_function.init { |_init_info| done = false } # Reset state

      table_function.execute do |_func_info, output|
        if done
          output.size = 0
        else
          # For now, just hardcode writing 3 rows
          value_vector = output.get_vector(0)

          value_vector.assign_string_element(0, 'test')
          value_vector.assign_string_element(1, 'test')
          value_vector.assign_string_element(2, 'test')

          output.size = 3
          done = true
        end
      end

      table_function
    end
    # rubocop:enable Metrics/MethodLength, Metrics/AbcSize

    def create_minimal_function
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_func'

      table_function.bind do |bind_info|
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      table_function.init { |_init_info| } # Empty init
      table_function.execute { |_func_info, output| output.size = 0 }

      table_function
    end
  end
end
