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

    # --- foldable? ---
    # Expression#foldable? returns true when the expression can be evaluated
    # at planning time (literal constants, constant arithmetic), and false
    # when it depends on runtime data (column references, non-deterministic
    # functions).

    def test_foldable_is_true_for_literal_integer
      expr, _ctx = bind_argument_of('test_foldable_int', :integer, 'SELECT test_foldable_int(42)')

      assert_predicate expr, :foldable?
    end

    def test_foldable_is_true_for_literal_varchar
      expr, _ctx = bind_argument_of('test_foldable_str', :varchar, "SELECT test_foldable_str('hello')")

      assert_predicate expr, :foldable?
    end

    def test_foldable_is_true_for_constant_arithmetic
      expr, _ctx = bind_argument_of('test_foldable_arith', :bigint, 'SELECT test_foldable_arith((40 + 2)::BIGINT)')

      assert_predicate expr, :foldable?
    end

    def test_foldable_is_false_for_column_reference
      @conn.execute('CREATE TABLE t_foldable_col (x INTEGER)')
      @conn.execute('INSERT INTO t_foldable_col VALUES (1)')
      expr, _ctx = bind_argument_of('test_foldable_col', :integer, 'SELECT test_foldable_col(x) FROM t_foldable_col')

      refute_predicate expr, :foldable?
    end

    def test_foldable_is_false_for_non_deterministic_function
      expr, _ctx = bind_argument_of('test_foldable_rand', :double, 'SELECT test_foldable_rand(random())')

      refute_predicate expr, :foldable?
    end

    # --- fold ---
    # Expression#fold(client_context) evaluates a foldable expression at
    # planning time and returns a DuckDB::Value holding the constant result.
    # The client_context is obtained from bind_info.client_context inside the
    # scalar function bind callback.
    # Calling fold on a non-foldable expression raises DuckDB::Error.
    #
    # Usage:
    #   sf.set_bind do |bind_info|
    #     expr           = bind_info.get_argument(0)
    #     client_context = bind_info.client_context
    #     value          = expr.fold(client_context)   # => DuckDB::Value
    #   end

    def test_fold_returns_value_for_integer_literal
      expr, client_context = bind_argument_of('test_fold_int', :integer, 'SELECT test_fold_int(42)')

      value = expr.fold(client_context)

      assert_kind_of DuckDB::Value, value
    end

    def test_fold_returns_value_for_varchar_literal
      expr, client_context = bind_argument_of('test_fold_str', :varchar, "SELECT test_fold_str('hello')")

      value = expr.fold(client_context)

      assert_kind_of DuckDB::Value, value
    end

    def test_fold_returns_value_for_constant_arithmetic
      expr, client_context = bind_argument_of('test_fold_arith', :bigint, 'SELECT test_fold_arith((40 + 2)::BIGINT)')

      value = expr.fold(client_context)

      assert_kind_of DuckDB::Value, value
    end

    def test_fold_raises_for_non_foldable_expression
      skip 'not implemented yet'
      @conn.execute('CREATE TABLE t_fold_col (x INTEGER)')
      @conn.execute('INSERT INTO t_fold_col VALUES (1)')
      expr, client_context = bind_argument_of('test_fold_col', :integer, 'SELECT test_fold_col(x) FROM t_fold_col')

      assert_raises(DuckDB::Error) { expr.fold(client_context) }
    end

    private

    # Registers a pass-through scalar function, executes sql, and returns
    # [expression, client_context] captured from the first argument during bind.
    def bind_argument_of(func_name, type, sql)
      expr = ctx = nil
      sf = build_scalar_function(func_name, type)
      sf.set_bind do |b|
        expr = b.get_argument(0)
        ctx = b.client_context
      end
      @conn.register_scalar_function(sf)
      @conn.execute(sql)
      [expr, ctx]
    end

    def build_scalar_function(func_name, type)
      sf = DuckDB::ScalarFunction.new
      sf.name = func_name
      sf.return_type = type
      sf.add_parameter(type)
      sf.set_function { |v| v }
      sf
    end
  end
end
