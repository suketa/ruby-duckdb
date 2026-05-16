# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class AggregateFunctionSetTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
    end

    def teardown
      @con&.close
      @db&.close
    end

    def test_initialize_with_string_name
      set = DuckDB::AggregateFunctionSet.new('my_agg')

      assert_instance_of DuckDB::AggregateFunctionSet, set
    end

    def test_initialize_with_symbol_name
      set = DuckDB::AggregateFunctionSet.new(:my_agg)

      assert_instance_of DuckDB::AggregateFunctionSet, set
    end

    def test_initialize_raises_with_no_argument
      assert_raises(ArgumentError) { DuckDB::AggregateFunctionSet.new }
    end

    def test_initialize_raises_with_invalid_type
      assert_raises(TypeError) { DuckDB::AggregateFunctionSet.new(1) }
    end
  end
end
