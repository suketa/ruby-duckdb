# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ScalarFunctionBindInfoTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
    end

    def teardown
      @conn.disconnect
      @db.close
    end

    # set_bind stores a block and returns self (the ScalarFunction)
    def test_set_bind_returns_self
      skip 'set_bind not implemented yet'
      sf = DuckDB::ScalarFunction.new
      result = sf.set_bind { |_bind_info| }

      assert_same sf, result
    end

    # set_bind raises ArgumentError when called without a block
    def test_set_bind_without_block_raises_error
      skip 'set_bind not implemented yet'
      sf = DuckDB::ScalarFunction.new

      assert_raises(ArgumentError) { sf.set_bind }
    end

    # The bind block is called at least once at query planning time
    def test_set_bind_block_is_called_at_planning_time
      skip 'set_bind not implemented yet'
      call_count = 0

      sf = DuckDB::ScalarFunction.new
      sf.name = 'test_bind_called'
      sf.return_type = :integer
      sf.add_parameter(:integer)
      sf.set_bind { |_bind_info| call_count += 1 }
      sf.set_function { |v| v }
      @conn.register_scalar_function(sf)
      @conn.execute('SELECT test_bind_called(1)')

      assert call_count >= 1
    end

    # The block receives a DuckDB::ScalarFunction::BindInfo instance
    def test_set_bind_block_receives_bind_info_object
      skip 'set_bind not implemented yet'
      received = nil

      sf = DuckDB::ScalarFunction.new
      sf.name = 'test_bind_info_type'
      sf.return_type = :integer
      sf.add_parameter(:integer)
      sf.set_bind { |bind_info| received = bind_info }
      sf.set_function { |v| v }
      @conn.register_scalar_function(sf)
      @conn.execute('SELECT test_bind_info_type(1)')

      assert_instance_of DuckDB::ScalarFunction::BindInfo, received
    end

    # argument_count returns 1 when one parameter was added
    def test_bind_info_argument_count_single_param
      skip 'set_bind not implemented yet'
      arg_count = nil

      sf = DuckDB::ScalarFunction.new
      sf.name = 'test_bind_argc_1'
      sf.return_type = :integer
      sf.add_parameter(:integer)
      sf.set_bind { |bind_info| arg_count = bind_info.argument_count }
      sf.set_function { |v| v }
      @conn.register_scalar_function(sf)
      @conn.execute('SELECT test_bind_argc_1(42)')

      assert_equal 1, arg_count
    end

    # argument_count returns 2 when two parameters were added
    def test_bind_info_argument_count_multiple_params
      skip 'set_bind not implemented yet'
      arg_count = nil

      sf = DuckDB::ScalarFunction.new
      sf.name = 'test_bind_argc_2'
      sf.return_type = :integer
      sf.add_parameter(:integer)
      sf.add_parameter(:integer)
      sf.set_bind { |bind_info| arg_count = bind_info.argument_count }
      sf.set_function { |a, b| a + b }
      @conn.register_scalar_function(sf)
      @conn.execute('SELECT test_bind_argc_2(1, 2)')

      assert_equal 2, arg_count
    end

    # set_error causes query execution to raise DuckDB::Error
    def test_bind_info_set_error_raises_on_execute
      skip 'set_bind not implemented yet'
      sf = DuckDB::ScalarFunction.new
      sf.name = 'test_bind_set_error'
      sf.return_type = :integer
      sf.add_parameter(:integer)
      sf.set_bind { |bind_info| bind_info.set_error('bind validation failed') }
      sf.set_function { |v| v }
      @conn.register_scalar_function(sf)

      assert_raises(DuckDB::Error) { @conn.execute('SELECT test_bind_set_error(1)') }
    end

    # An exception raised inside the bind block is reported as a DuckDB::Error
    def test_bind_block_exception_is_reported_as_error
      skip 'set_bind not implemented yet'
      sf = DuckDB::ScalarFunction.new
      sf.name = 'test_bind_exception'
      sf.return_type = :integer
      sf.add_parameter(:integer)
      sf.set_bind { |_bind_info| raise StandardError, 'unexpected bind error' }
      sf.set_function { |v| v }
      @conn.register_scalar_function(sf)

      assert_raises(DuckDB::Error) { @conn.execute('SELECT test_bind_exception(1)') }
    end
  end
end
