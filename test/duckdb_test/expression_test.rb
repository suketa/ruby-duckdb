# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ExpressionTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
    end

    def teardown
      @conn.disconnect
      @db.close
    end

    # Helper: register a scalar function and capture the expression at index 0 via bind
    def capture_expression(func_name, param_type, &query_block)
      expr = nil
      sf = DuckDB::ScalarFunction.new
      sf.name = func_name
      sf.return_type = param_type
      sf.add_parameter(param_type)
      sf.set_bind { |bind_info| expr = bind_info.get_argument(0) }
      sf.set_function { |v| v }
      @conn.register_scalar_function(sf)
      query_block.call
      expr
    end

    # foldable? returns true for a literal constant argument
    def test_foldable_returns_true_for_literal_integer
      expr = capture_expression('test_foldable_int', :integer) do
        @conn.execute('SELECT test_foldable_int(42)')
      end

      assert_predicate expr, :foldable?
    end

    # foldable? returns true for a literal varchar constant
    def test_foldable_returns_true_for_literal_varchar
      expr = capture_expression('test_foldable_str', :varchar) do
        @conn.execute("SELECT test_foldable_str('hello')")
      end

      assert_predicate expr, :foldable?
    end

    # foldable? returns false for a column reference (non-constant)
    def test_foldable_returns_false_for_column_reference
      @conn.execute('CREATE TABLE test_foldable_col_t (x INTEGER)')
      @conn.execute('INSERT INTO test_foldable_col_t VALUES (1)')

      expr = capture_expression('test_foldable_col', :integer) do
        @conn.execute('SELECT test_foldable_col(x) FROM test_foldable_col_t')
      end

      refute_predicate expr, :foldable?
    end

    # foldable? returns true for a constant arithmetic expression (e.g. 40 + 2)
    def test_foldable_returns_true_for_constant_arithmetic
      expr = capture_expression('test_foldable_arith', :bigint) do
        @conn.execute('SELECT test_foldable_arith((40 + 2)::BIGINT)')
      end

      assert_predicate expr, :foldable?
    end

    # foldable? returns false for a non-deterministic function like random()
    def test_foldable_returns_false_for_random
      expr = capture_expression('test_foldable_random', :double) do
        @conn.execute('SELECT test_foldable_random(random())')
      end

      refute_predicate expr, :foldable?
    end
  end
end
