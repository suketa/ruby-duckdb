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

    # --- helpers -----------------------------------------------------------

    def make_af(type = :bigint)
      af = DuckDB::AggregateFunction.new
      af.name = 'test_agg'
      af.return_type = DuckDB::LogicalType.const_get(type.to_s.upcase)
      af.add_parameter(DuckDB::LogicalType.const_get(type.to_s.upcase))
      af.set_init   { 0 }
      af.set_update  { |state, val| state + val }
      af.set_combine { |s1, s2| s1 + s2 }
      af
    end

    # --- add ---------------------------------------------------------------

    def test_add_returns_self
      af  = make_af(:bigint)
      set = DuckDB::AggregateFunctionSet.new('test_agg')

      assert_same set, set.add(af)
    end

    def test_add_raises_with_non_aggregate_function
      set = DuckDB::AggregateFunctionSet.new('test_agg')

      assert_raises(TypeError) { set.add('not an aggregate function') }
    end

    def test_add_accepts_duplicate_overload
      af1 = make_af(:bigint)
      af2 = make_af(:bigint)
      set = DuckDB::AggregateFunctionSet.new('test_agg')
      set.add(af1)

      assert_same set, set.add(af2)
    end

    def test_add_multiple_overloads_with_different_parameter_types
      af_bigint = make_af(:bigint)
      af_double = make_af(:double)
      set = DuckDB::AggregateFunctionSet.new('test_agg')

      assert_same set, set.add(af_bigint)
      assert_same set, set.add(af_double)
    end
  end
end
