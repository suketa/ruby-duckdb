# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class TableFunctionTest < Minitest::Test
    # Test 1: Create and destroy
    def test_create_and_destroy
      tf = DuckDB::TableFunction.create

      assert_instance_of DuckDB::TableFunction, tf
      tf.destroy
    end

    # Test 2: Double destroy should not crash
    def test_double_destroy
      tf = DuckDB::TableFunction.create
      tf.destroy
      tf.destroy # Should be safe
    end

    # Test 3: Set name
    def test_set_name
      tf = DuckDB::TableFunction.create
      tf.name = 'my_function'
      tf.destroy
    end

    # Test 4: Add positional parameter
    def test_add_parameter
      tf = DuckDB::TableFunction.create
      tf.name = 'my_function'
      tf.add_parameter(DuckDB::LogicalType::BIGINT)
      tf.destroy
    end

    # Test 5: Add named parameter
    def test_add_named_parameter
      tf = DuckDB::TableFunction.create
      tf.name = 'my_function'
      tf.add_named_parameter('limit', DuckDB::LogicalType::BIGINT)
      tf.destroy
    end

    # Test 6: Block form for auto-cleanup
    def test_create_with_block
      result = DuckDB::TableFunction.create do |tf|
        tf.name = 'my_function'
        :block_result
      end

      assert_equal :block_result, result
    end

    # Test 7: Register without callbacks (should fail gracefully)
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
      database = DuckDB::Database.new
      conn = database.connect
      table_function = DuckDB::TableFunction.create
      table_function.name = 'incomplete_function'
      table_function.add_parameter(DuckDB::LogicalType::BIGINT)
      [database, conn, table_function]
    end

    def cleanup_function(table_function, conn, database)
      table_function.destroy
      conn.disconnect
      database.close
    end
  end
end
