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

    def test_init_info_set_max_threads
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_init_max_threads'

      result = table_function.init do |init_info|
        init_info.set_max_threads(4)
      end

      assert_equal table_function, result
    end

    def test_init_info_max_threads_setter
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_init_max_threads_setter'

      result = table_function.init do |init_info|
        init_info.max_threads = 4
      end

      assert_equal table_function, result
    end

    def test_init_info_column_count
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_init_column_count'

      table_function.bind do |bind_info|
        bind_info.add_result_column('id', DuckDB::LogicalType::BIGINT)
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      observed_column_count = nil
      table_function.init do |init_info|
        observed_column_count = init_info.column_count
      end

      done = false
      table_function.execute do |_func_info, output|
        if done
          output.size = 0
        else
          output.set_value(0, 0, 1)
          output.set_value(1, 0, 10)
          output.size = 1
          done = true
        end
      end

      @connection.register_table_function(table_function)
      @connection.query('SELECT * FROM test_init_column_count()').each.to_a

      assert_equal 2, observed_column_count
    end

    def test_init_info_column_index
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_init_column_index'

      table_function.bind do |bind_info|
        bind_info.add_result_column('id', DuckDB::LogicalType::BIGINT)
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      observed_column_indexes = nil
      table_function.init do |init_info|
        observed_column_indexes = [init_info.column_index(0), init_info.column_index(1)]
      end

      done = false
      table_function.execute do |_func_info, output|
        if done
          output.size = 0
        else
          output.set_value(0, 0, 1)
          output.set_value(1, 0, 10)
          output.size = 1
          done = true
        end
      end

      @connection.register_table_function(table_function)
      @connection.query('SELECT * FROM test_init_column_index()').each.to_a

      assert_equal [0, 1], observed_column_indexes
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
