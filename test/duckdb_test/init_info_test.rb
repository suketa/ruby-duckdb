# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class InitInfoTest < Minitest::Test
    def setup
      @database = DuckDB::Database.open
      @connection = @database.connect
    end

    def teardown
      @connection.disconnect
      @database.close
    end

    # Test 1: Init callback setup
    def test_init_callback
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_init'

      result = table_function.bind do |bind_info|
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      result2 = table_function.init do |_init_info|
        # Will be tested in integration tests (Phase 6)
      end

      assert_equal table_function, result
      assert_equal table_function, result2
    end

    # Test 2: Init without block raises error
    def test_init_without_block
      table_function = DuckDB::TableFunction.new

      error = assert_raises(ArgumentError) do
        table_function.init
      end

      assert_equal 'block is required for init', error.message
    end

    # Test 3: InitInfo set_error
    def test_init_info_set_error
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_init_error'

      result = table_function.init do |init_info|
        # Verify method exists
        result = init_info.set_error('Test error')

        assert_equal init_info, result
      end

      # NOTE: Actual error reporting will be tested in Phase 6
      assert_equal table_function, result
    end
  end
end
