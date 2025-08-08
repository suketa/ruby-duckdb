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
  end
end
