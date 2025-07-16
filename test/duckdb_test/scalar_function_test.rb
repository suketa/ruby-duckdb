# frozen_string_literal: true

require 'test_helper'
require 'time'

module DuckDBTest
  class ScalarFunctionTest < Minitest::Test
    def test_initialize
      sf = DuckDB::ScalarFunction.new
      assert_instance_of DuckDB::ScalarFunction, sf
    end
  end
end
