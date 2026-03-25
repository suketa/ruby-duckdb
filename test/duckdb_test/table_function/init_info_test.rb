# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class TableFunctionInitInfoTest < Minitest::Test
    def setup
      @database = DuckDB::Database.open
      @connection = @database.connect
    end

    def teardown
      @connection.disconnect
      @database.close
    end

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

    def test_init_without_block
      table_function = DuckDB::TableFunction.new

      error = assert_raises(ArgumentError) do
        table_function.init
      end

      assert_equal 'block is required for init', error.message
    end

    def test_init_info_set_error
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_init_error'

      result = table_function.init do |init_info|
        # Verify set_error method exists and returns self
        # NOTE: This block is stored but not invoked here
        # Actual execution and error reporting will be tested in Phase 6
        init_info.set_error('Test error')
      end

      assert_equal table_function, result
    end

    def test_init_info_alias
      assert_same DuckDB::TableFunction::InitInfo, DuckDB::InitInfo
    end

    def test_init_info_alias_deprecation_warning
      DuckDB.send(:remove_const, :InitInfo) if DuckDB.const_defined?(:InitInfo, false)
      warning = capture_io { DuckDB::InitInfo }.last

      assert_match(/deprecated/, warning)
    ensure
      DuckDB.send(:remove_const, :InitInfo) if DuckDB.const_defined?(:InitInfo, false)
    end
  end
end
