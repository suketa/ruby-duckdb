# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class LogicalTypeTest < Minitest::Test
    def test_defined_klass
      assert(DuckDB.const_defined?(:LogicalType))
    end
  end
end
