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
      sf = DuckDB::ScalarFunction.new
      result = sf.set_bind { |_bind_info| nil }

      assert_same sf, result
    end

    # set_bind raises ArgumentError when called without a block
    def test_set_bind_without_block_raises_error
      sf = DuckDB::ScalarFunction.new

      assert_raises(ArgumentError) { sf.set_bind }
    end

    # The bind block is called exactly once at query planning time (not per-row)
    def test_set_bind_block_is_called_at_planning_time
      call_count = 0

      sf = DuckDB::ScalarFunction.new
      sf.name = 'test_bind_called'
      sf.return_type = :integer
      sf.add_parameter(:integer)
      sf.set_bind { |_bind_info| call_count += 1 }
      sf.set_function { |v| v }
      @conn.register_scalar_function(sf)
      @conn.execute('SELECT test_bind_called(1)')

      assert_equal 1, call_count
    end

    # The block receives a DuckDB::ScalarFunction::BindInfo instance
    def test_set_bind_block_receives_bind_info_object
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
    def test_bind_info_argument_count_multiple_params # rubocop:disable Metrics/MethodLength
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

    # An exception raised inside the bind block is reported as a DuckDB::Error
    def test_bind_block_exception_is_reported_as_error
      sf = DuckDB::ScalarFunction.new
      sf.name = 'test_bind_exception'
      sf.return_type = :integer
      sf.add_parameter(:integer)
      sf.set_bind { |_bind_info| raise StandardError, 'unexpected bind error' }
      sf.set_function { |v| v }
      @conn.register_scalar_function(sf)

      assert_raises(DuckDB::Error) { @conn.execute('SELECT test_bind_exception(1)') }
    end

    # The exception message from the bind block is included in the DuckDB::Error
    def test_bind_block_exception_message_is_propagated
      sf = DuckDB::ScalarFunction.new
      sf.name = 'test_bind_exception_msg'
      sf.return_type = :integer
      sf.add_parameter(:integer)
      sf.set_bind { |_bind_info| raise ArgumentError, 'custom bind failure' }
      sf.set_function { |v| v }
      @conn.register_scalar_function(sf)

      error = assert_raises(DuckDB::Error) { @conn.execute('SELECT test_bind_exception_msg(1)') }
      assert_match(/custom bind failure/, error.message)
    end

    # argument_count returns 0 for a zero-parameter function
    def test_bind_info_argument_count_zero_params
      arg_count = nil

      sf = DuckDB::ScalarFunction.new
      sf.name = 'test_bind_argc_0'
      sf.return_type = :integer
      sf.set_bind { |bind_info| arg_count = bind_info.argument_count }
      sf.set_function { 42 }
      @conn.register_scalar_function(sf)
      @conn.execute('SELECT test_bind_argc_0()')

      assert_equal 0, arg_count
    end

    # bind callback works correctly in a WHERE clause
    def test_set_bind_in_where_clause # rubocop:disable Metrics/MethodLength
      called = false

      sf = DuckDB::ScalarFunction.new
      sf.name = 'test_bind_where'
      sf.return_type = :boolean
      sf.add_parameter(:integer)
      sf.set_bind { |_bind_info| called = true }
      sf.set_function(&:positive?)
      @conn.register_scalar_function(sf)

      result = @conn.execute('SELECT true WHERE test_bind_where(1)')

      assert called
      assert result.first.first
    end

    # practical use: use argument_count to validate and set_error early (varargs function)
    def test_set_bind_validates_argument_count_with_set_error # rubocop:disable Metrics/MethodLength,Metrics/AbcSize
      sf = DuckDB::ScalarFunction.new
      sf.name = 'test_bind_validate_argc'
      sf.return_type = :integer
      sf.varargs_type = :integer
      sf.set_bind do |bind_info|
        bind_info.set_error('at least 2 arguments required') if bind_info.argument_count < 2
      end
      sf.set_function { |*args| args.sum }
      @conn.register_scalar_function(sf)

      result = @conn.execute('SELECT test_bind_validate_argc(1, 2, 3)')

      assert_equal 6, result.first.first

      error = assert_raises(DuckDB::Error) { @conn.execute('SELECT test_bind_validate_argc(1)') }
      assert_match(/at least 2 arguments required/, error.message)
    end

    # --- get_argument: wraps duckdb_scalar_function_bind_get_argument ---

    # get_argument returns a DuckDB::Expression object for the argument at the given index
    def test_get_argument_returns_expression_object
      expr = nil

      sf = DuckDB::ScalarFunction.new
      sf.name = 'test_get_arg_class'
      sf.return_type = :integer
      sf.add_parameter(:integer)
      sf.set_bind { |bind_info| expr = bind_info.get_argument(0) }
      sf.set_function { |v| v }
      @conn.register_scalar_function(sf)
      @conn.execute('SELECT test_get_arg_class(1)')

      assert_instance_of DuckDB::Expression, expr
    end

    # get_argument raises for an out-of-range index
    def test_get_argument_raises_for_out_of_range_index
      sf = DuckDB::ScalarFunction.new
      sf.name = 'test_get_arg_oob'
      sf.return_type = :integer
      sf.add_parameter(:integer)
      sf.set_bind { |bind_info| bind_info.get_argument(1) }
      sf.set_function { |v| v }
      @conn.register_scalar_function(sf)

      assert_raises(DuckDB::Error) { @conn.execute('SELECT test_get_arg_oob(1)') }
    end

    # --- future: requires duckdb_scalar_function_set_bind_data / get_bind_data ---

    # set_bind_data stores data that can be retrieved during execute
    def test_bind_info_set_bind_data
      skip 'set_bind_data not implemented yet'
    end

    # --- client_context: wraps duckdb_scalar_function_get_client_context ---

    # client_context returns a DuckDB::ClientContext object from the bind callback
    def test_client_context_returns_client_context_object
      received = nil

      sf = DuckDB::ScalarFunction.new
      sf.name = 'test_bind_client_context_class'
      sf.return_type = :integer
      sf.add_parameter(:integer)
      sf.set_bind { |bind_info| received = bind_info.client_context }
      sf.set_function { |v| v }
      @conn.register_scalar_function(sf)
      @conn.execute('SELECT test_bind_client_context_class(1)')

      assert_instance_of DuckDB::ClientContext, received
    end
  end
end
