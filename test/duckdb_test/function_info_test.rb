# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class FunctionInfoTest < Minitest::Test
    def setup
      @database = DuckDB::Database.open
      @connection = @database.connect
    end

    def teardown
      @connection.disconnect
      @database.close
    end

    # Test 1: FunctionInfo set_error
    def test_function_info_set_error
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_error'
      table_function.add_parameter(DuckDB::LogicalType::BIGINT)

      result = table_function.bind do |bind_info|
        bind_info.add_result_column('id', DuckDB::LogicalType::BIGINT)
      end

      # NOTE: Can't test set_error until execute callback is implemented
      assert_equal table_function, result
    end

    # Test 2: Execute callback setup
    def test_execute_callback
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_execute'

      result = table_function.bind do |bind_info|
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      result2 = table_function.execute do |_function_info, _output|
        # Will be tested in integration tests (Phase 6)
      end

      assert_equal table_function, result
      assert_equal table_function, result2
    end

    # Test 3: Execute without block raises error
    def test_execute_without_block
      table_function = DuckDB::TableFunction.new

      error = assert_raises(ArgumentError) do
        table_function.execute
      end

      assert_equal 'block is required for execute', error.message
    end
  end
end
