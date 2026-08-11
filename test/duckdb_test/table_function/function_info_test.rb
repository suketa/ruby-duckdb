# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class TableFunctionFunctionInfoTest < Minitest::Test
    def setup
      @database = DuckDB::Database.open
      @connection = @database.connect
    end

    def teardown
      @connection.disconnect
      @database.close
    end

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

    def test_bind_data_round_trip
      skip 'GC.compact hangs on Windows in parallel test execution' if Gem.win_platform?

      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_bind_data'

      table_function.bind do |bind_info|
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
        bind_info.bind_data = { token: 'round-trip', n: 7 }
      end

      table_function.init { |_init_info| GC.compact }

      observed_bind_data = nil
      table_function.execute do |func_info, output|
        observed_bind_data = func_info.bind_data
        output.size = 0
      end

      @connection.register_table_function(table_function)
      @connection.query('SELECT * FROM test_bind_data()').each.to_a

      assert_equal({ token: 'round-trip', n: 7 }, observed_bind_data)
    end

    def test_bind_data_last_set_wins
      skip 'GC.compact hangs on Windows in parallel test execution' if Gem.win_platform?

      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_bind_data_overwrite'

      table_function.bind do |bind_info|
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
        bind_info.bind_data = { which: 'first' }
        bind_info.bind_data = { which: 'second' }
      end

      table_function.init { |_init_info| GC.compact }

      observed_bind_data = nil
      table_function.execute do |func_info, output|
        observed_bind_data = func_info.bind_data
        output.size = 0
      end

      @connection.register_table_function(table_function)
      @connection.query('SELECT * FROM test_bind_data_overwrite()').each.to_a

      assert_equal({ which: 'second' }, observed_bind_data)
    end

    def test_bind_data_nil_when_unset
      skip 'GC.compact hangs on Windows in parallel test execution' if Gem.win_platform?

      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_bind_data_unset'

      table_function.bind do |bind_info|
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      table_function.init { |_init_info| GC.compact }

      observed_bind_data = :unset
      table_function.execute do |func_info, output|
        observed_bind_data = func_info.bind_data
        output.size = 0
      end

      @connection.register_table_function(table_function)
      @connection.query('SELECT * FROM test_bind_data_unset()').each.to_a

      assert_nil observed_bind_data
    end

    def test_execute_without_block
      table_function = DuckDB::TableFunction.new

      error = assert_raises(ArgumentError) do
        table_function.execute
      end

      assert_equal 'block is required for execute', error.message
    end

    def test_function_info_alias
      assert_same DuckDB::TableFunction::FunctionInfo, DuckDB::FunctionInfo
    end

    def test_function_info_alias_deprecation_warning
      DuckDB.send(:remove_const, :FunctionInfo) if DuckDB.const_defined?(:FunctionInfo, false)
      warning = capture_io { DuckDB::FunctionInfo }.last

      assert_match(/deprecated/, warning)
    ensure
      DuckDB.send(:remove_const, :FunctionInfo) if DuckDB.const_defined?(:FunctionInfo, false)
    end
  end
end
