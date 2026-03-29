# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ScalarFunctionSetTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
    end

    def teardown
      @con&.close
      @db&.close
    end

    def test_initialize_with_string_name
      set = DuckDB::ScalarFunctionSet.new('add')

      assert_instance_of DuckDB::ScalarFunctionSet, set
    end

    def test_initialize_with_symbol_name
      set = DuckDB::ScalarFunctionSet.new(:add)

      assert_instance_of DuckDB::ScalarFunctionSet, set
    end

    def test_initialize_raises_with_no_argument
      assert_raises(ArgumentError) { DuckDB::ScalarFunctionSet.new }
    end

    def test_initialize_raises_with_invalid_type
      assert_raises(TypeError) { DuckDB::ScalarFunctionSet.new(1) }
    end

    def test_add_returns_self
      sf = DuckDB::ScalarFunction.create(return_type: :integer, parameter_types: [:integer, :integer]) { |a, b| a + b }
      set = DuckDB::ScalarFunctionSet.new(:add)

      assert_same set, set.add(sf)
    end

    def test_add_raises_with_non_scalar_function
      set = DuckDB::ScalarFunctionSet.new(:add)

      assert_raises(TypeError) { set.add('not a scalar function') }
    end

    def test_add_accepts_duplicate_overload
      sf1 = DuckDB::ScalarFunction.create(return_type: :integer, parameter_types: [:integer, :integer]) { |a, b| a + b }
      sf2 = DuckDB::ScalarFunction.create(return_type: :integer, parameter_types: [:integer, :integer]) { |a, b| a * b }
      set = DuckDB::ScalarFunctionSet.new(:add)
      set.add(sf1)

      assert_same set, set.add(sf2)
    end

    def test_add_multiple_overloads_with_different_parameter_types
      sf_int = DuckDB::ScalarFunction.create(return_type: :integer, parameter_types: [:integer, :integer]) { |a, b| a + b }
      sf_dbl = DuckDB::ScalarFunction.create(return_type: :double, parameter_types: [:double, :double]) { |a, b| a + b }
      sf_str = DuckDB::ScalarFunction.create(return_type: :varchar, parameter_types: [:varchar, :varchar]) { |a, b| "#{a}#{b}" }
      set = DuckDB::ScalarFunctionSet.new(:add)

      assert_same set, set.add(sf_int)
      assert_same set, set.add(sf_dbl)
      assert_same set, set.add(sf_str)
    end

    def test_add_overloads_with_different_parameter_counts
      sf_unary = DuckDB::ScalarFunction.create(return_type: :integer, parameter_types: [:integer]) { |a| a * 2 }
      sf_binary = DuckDB::ScalarFunction.create(return_type: :integer, parameter_types: [:integer, :integer]) { |a, b| a + b }
      set = DuckDB::ScalarFunctionSet.new(:my_func)

      assert_same set, set.add(sf_unary)
      assert_same set, set.add(sf_binary)
    end

    def test_add_overload_with_varargs_type
      sf_varargs = DuckDB::ScalarFunction.create(return_type: :integer, varargs_type: :integer) { |*args| args.sum }
      set = DuckDB::ScalarFunctionSet.new(:sum_all)

      assert_same set, set.add(sf_varargs)
    end
  end
end
