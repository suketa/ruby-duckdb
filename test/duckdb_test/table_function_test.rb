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
