# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ScalarFunctionTest < Minitest::Test
    def test_initialize
      sf = DuckDB::ScalarFunction.new
      assert_instance_of DuckDB::ScalarFunction, sf
    end

    def test_set_name
      sf = DuckDB::ScalarFunction.new
      sf1 = sf.set_name('test_function')
      assert_instance_of DuckDB::ScalarFunction, sf1
      assert_equal sf1.__id__, sf.__id__
    end

    def test_set_return_type
      sf = DuckDB::ScalarFunction.new
      logical_type = DuckDB::LogicalType.new(4) # DUCKDB_TYPE_INTEGER
      sf1 = sf.set_return_type(logical_type)
      assert_instance_of DuckDB::ScalarFunction, sf1
      assert_equal sf1.__id__, sf.__id__
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

      sf = DuckDB::ScalarFunction.new
      sf.set_name('foo')
      sf.set_return_type(DuckDB::LogicalType.new(4)) # INTEGER
      sf.set_function { 1 }

      con.register_scalar_function(sf)

      result = con.execute('SELECT foo()')
      assert_equal 1, result.first.first
    ensure
      con&.close
      db&.close
    end
  end
end
