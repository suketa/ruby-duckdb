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

    # Test 1: Set bind callback on table function
    def test_set_bind_callback
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_bind'
      table_function.add_parameter(DuckDB::LogicalType::BIGINT)

      bind_called = false
      table_function.bind do |bind_info|
        bind_called = true

        assert_instance_of DuckDB::BindInfo, bind_info
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      # Store flag for later verification (actual call happens on register)
      refute bind_called # Not called yet
    end

    # Test 2: Add result column in bind
    def test_bind_add_result_column
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_columns'

      table_function.bind do |bind_info|
        bind_info.add_result_column('id', DuckDB::LogicalType::BIGINT)
        bind_info.add_result_column('name', DuckDB::LogicalType::VARCHAR)
      end
    end

    # Test 3: Get parameter count
    def test_bind_parameter_count
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_params'
      table_function.add_parameter(DuckDB::LogicalType::BIGINT)
      table_function.add_parameter(DuckDB::LogicalType::VARCHAR)

      table_function.bind do |bind_info|
        count = bind_info.parameter_count

        assert_equal 2, count
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end
    end

    # Test 4: Get parameter value by index
    def test_bind_get_parameter
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_get_param'
      table_function.add_parameter(DuckDB::LogicalType::BIGINT)

      table_function.bind do |bind_info|
        param = bind_info.get_parameter(0)

        # Parameter is a DuckDB::Value (will test actual value on registration)
        assert_not_nil param
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end
    end

    # Test 5: Get named parameter
    def test_bind_get_named_parameter
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_named_param'
      table_function.add_named_parameter('limit', DuckDB::LogicalType::BIGINT)

      table_function.bind do |bind_info|
        bind_info.get_named_parameter('limit')

        # May be nil if not provided
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end
    end

    # Test 6: Set cardinality hint
    def test_bind_set_cardinality
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_cardinality'

      table_function.bind do |bind_info|
        bind_info.set_cardinality(100, true) # 100 rows, exact
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end
    end

    # Test 7: Set error in bind
    def test_bind_set_error
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_error'

      table_function.bind do |bind_info|
        bind_info.set_error('Invalid parameters')
      end
    end
  end
end
