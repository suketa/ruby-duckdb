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

    # Test 1: Simple table function returning empty result
    def test_simple_table_function
      table_function = create_simple_function

      @connection.register_table_function(table_function)
      result = @connection.query('SELECT * FROM simple_function()')

      assert_equal 0, result.count
    end

    # Test 2: Table function with parameters
    def test_table_function_with_parameters
      table_function = create_parameterized_function

      @connection.register_table_function(table_function)
      result = @connection.query("SELECT * FROM repeat_string('hello', 3)")

      assert_equal 0, result.count # Placeholder test
    end

    # Test 3: Connection#register_table_function without block
    def test_register_table_function_without_block
      table_function = create_minimal_function

      result = @connection.register_table_function(table_function)

      assert_equal @connection, result
    end

    private

    # rubocop:disable Metrics/MethodLength
    def create_simple_function
      table_function = DuckDB::TableFunction.new
      table_function.name = 'simple_function'

      table_function.bind do |bind_info|
        bind_info.add_result_column('id', DuckDB::LogicalType::BIGINT)
        bind_info.add_result_column('name', DuckDB::LogicalType::VARCHAR)
      end

      table_function.init { |_init_info| } # Empty init

      table_function.execute do |_func_info, output|
        output.size = 0 # Return 0 rows for now
      end

      table_function
    end

    def create_parameterized_function
      table_function = DuckDB::TableFunction.new
      table_function.name = 'repeat_string'
      table_function.add_parameter(DuckDB::LogicalType::VARCHAR)
      table_function.add_parameter(DuckDB::LogicalType::BIGINT)

      table_function.bind do |bind_info|
        bind_info.add_result_column('value', DuckDB::LogicalType::VARCHAR)
      end

      table_function.init { |_init_info| } # Empty init

      table_function.execute do |_func_info, output|
        output.size = 0 # For now, just output empty result
      end

      table_function
    end
    # rubocop:enable Metrics/MethodLength

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
