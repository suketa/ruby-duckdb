# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class BindInfoTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
    end

    def teardown
      @conn.disconnect
      @db.close
    end

    # Test 1: Bind method accepts block and returns self
    def test_set_bind_callback
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_bind'
      table_function.add_parameter(DuckDB::LogicalType::BIGINT)

      result = table_function.bind do |bind_info|
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      assert_equal table_function, result
    end

    # Test 2: Add result column method works
    def test_bind_add_result_column
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_columns'

      result = table_function.bind do |bind_info|
        bind_info.add_result_column('id', DuckDB::LogicalType::BIGINT)
        bind_info.add_result_column('name', DuckDB::LogicalType::VARCHAR)
      end

      assert_equal table_function, result
    end

    # Test 3: Parameter count method exists
    def test_bind_parameter_count
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_params'
      table_function.add_parameter(DuckDB::LogicalType::BIGINT)
      table_function.add_parameter(DuckDB::LogicalType::VARCHAR)

      result = table_function.bind do |bind_info|
        bind_info.parameter_count
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      assert_equal table_function, result
    end

    # Test 4: Get parameter method accepts index
    def test_bind_get_parameter
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_get_param'
      table_function.add_parameter(DuckDB::LogicalType::BIGINT)

      result = table_function.bind do |bind_info|
        bind_info.get_parameter(0)
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      assert_equal table_function, result
    end

    # Test 5: Get named parameter method accepts name
    def test_bind_get_named_parameter
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_named_param'
      table_function.add_parameter(DuckDB::LogicalType::BIGINT)
      table_function.add_named_parameter('limit', DuckDB::LogicalType::BIGINT)

      result = table_function.bind do |bind_info|
        bind_info.get_named_parameter('limit')
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      assert_equal table_function, result
    end

    # Test 6: Set cardinality method works
    def test_bind_set_cardinality
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_cardinality'

      result = table_function.bind do |bind_info|
        bind_info.set_cardinality(100, true)
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      assert_equal table_function, result
    end

    # Test 7: Set error method works
    def test_bind_set_error
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_error'

      result = table_function.bind do |bind_info|
        bind_info.set_error('Invalid parameters')
      end

      assert_equal table_function, result
    end

    # Test 8: Bind without block raises ArgumentError
    def test_bind_without_block_raises_error
      table_function = DuckDB::TableFunction.new

      assert_raises(ArgumentError) do
        table_function.bind
      end
    end

    # Test 9: Exception in bind block is caught safely
    def test_bind_exception_handling
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_exception'

      # Should not crash - exception should be caught by rb_protect
      result = table_function.bind do |_bind_info|
        raise StandardError, 'Test exception in bind'
      end

      assert_equal table_function, result
    end
  end
end
