# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ScalarFunctionTest < Minitest::Test
    def test_initialize
      sf = DuckDB::ScalarFunction.new
      assert_instance_of DuckDB::ScalarFunction, sf
    end

    def test_name_setter
      sf = DuckDB::ScalarFunction.new
      sf.name = 'test_function'
      assert_instance_of DuckDB::ScalarFunction, sf
    end

    def test_return_type_setter
      sf = DuckDB::ScalarFunction.new
      logical_type = DuckDB::LogicalType.new(4) # DUCKDB_TYPE_INTEGER
      sf.return_type = logical_type
      assert_instance_of DuckDB::ScalarFunction, sf
    end

    def test_return_type_setter_raises_error_for_unsupported_type
      sf = DuckDB::ScalarFunction.new
      varchar_type = DuckDB::LogicalType.new(17) # DUCKDB_TYPE_VARCHAR

      error = assert_raises(DuckDB::Error) do
        sf.return_type = varchar_type
      end

      assert_match(/only.*INTEGER.*supported/i, error.message)
    end

    def test_set_function
      sf = DuckDB::ScalarFunction.new
      sf1 = sf.set_function { 1 }
      assert_instance_of DuckDB::ScalarFunction, sf1
      assert_equal sf1.__id__, sf.__id__
    end

    def test_register_scalar_function
      db = DuckDB::Database.open
      con = db.connect

      # Scalar functions with Ruby callbacks require single-threaded execution
      con.execute('SET threads=1')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'foo'
      sf.return_type = DuckDB::LogicalType.new(4) # INTEGER
      sf.set_function { 1 }

      con.register_scalar_function(sf)

      result = con.execute('SELECT foo()')
      assert_equal 1, result.first.first
    ensure
      con&.close
      db&.close
    end

    def test_register_scalar_function_raises_error_without_single_thread
      db = DuckDB::Database.open
      con = db.connect

      sf = DuckDB::ScalarFunction.new
      sf.name = 'will_fail'
      sf.return_type = DuckDB::LogicalType.new(4) # INTEGER
      sf.set_function { 1 }

      # Should raise error because threads is not 1
      error = assert_raises(DuckDB::Error) do
        con.register_scalar_function(sf)
      end

      assert_match(/single-threaded execution/, error.message)
      assert_match(/SET threads=1/, error.message)
    ensure
      con&.close
      db&.close
    end
  end
end
