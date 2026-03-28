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
    # planning time and returns a native Ruby object holding the constant result.
    # The client_context is obtained from bind_info.client_context inside the
    # scalar function bind callback.
    # Calling fold on a non-foldable expression raises DuckDB::Error.
    #
    # Usage:
    #   sf.set_bind do |bind_info|
    #     expr           = bind_info.get_argument(0)
    #     client_context = bind_info.client_context
    #     value          = expr.fold(client_context)   # => Integer, String, Float, ...
    #   end

    def test_fold_returns_integer_for_integer_literal
      expr, client_context = bind_argument_of('test_fold_int', :integer, 'SELECT test_fold_int(42)')

      value = expr.fold(client_context)

      assert_equal 42, value
    end

    def test_fold_returns_string_for_varchar_literal
      expr, client_context = bind_argument_of('test_fold_str', :varchar, "SELECT test_fold_str('hello')")

      value = expr.fold(client_context)

      assert_equal 'hello', value
    end

    def test_fold_returns_integer_for_constant_arithmetic
      expr, client_context = bind_argument_of('test_fold_arith', :bigint, 'SELECT test_fold_arith((40 + 2)::BIGINT)')

      value = expr.fold(client_context)

      assert_equal 42, value
    end

    def test_fold_raises_for_non_foldable_expression
      @conn.execute('CREATE TABLE t_fold_col (x INTEGER)')
      @conn.execute('INSERT INTO t_fold_col VALUES (1)')
      expr, client_context = bind_argument_of('test_fold_col', :integer, 'SELECT test_fold_col(x) FROM t_fold_col')

      assert_raises(DuckDB::Error) { expr.fold(client_context) }
    end

    def test_fold_returns_time_for_timestamp_literal # rubocop:disable Minitest/MultipleAssertions, Metrics/AbcSize, Metrics/MethodLength
      expr, client_context = bind_argument_of(
        'test_fold_ts', :timestamp,
        "SELECT test_fold_ts('2025-01-15 12:34:56.123456'::TIMESTAMP)"
      )

      value = expr.fold(client_context)

      assert_instance_of Time, value
      assert_equal 2025,    value.year
      assert_equal 1,       value.month
      assert_equal 15,      value.day
      assert_equal 12,      value.hour
      assert_equal 34,      value.min
      assert_equal 56,      value.sec
      assert_equal 123_456, value.usec
    end

    def test_fold_returns_date_for_date_literal # rubocop:disable Minitest/MultipleAssertions
      expr, client_context = bind_argument_of(
        'test_fold_date', :date,
        "SELECT test_fold_date('2025-06-30'::DATE)"
      )

      value = expr.fold(client_context)

      assert_instance_of Date, value
      assert_equal 2025, value.year
      assert_equal 6,    value.month
      assert_equal 30,   value.day
    end

    def test_fold_returns_time_for_time_literal # rubocop:disable Minitest/MultipleAssertions
      expr, client_context = bind_argument_of(
        'test_fold_time', :time,
        "SELECT test_fold_time('12:34:56.123456'::TIME)"
      )

      value = expr.fold(client_context)

      assert_instance_of Time, value
      assert_equal 12,      value.hour
      assert_equal 34,      value.min
      assert_equal 56,      value.sec
      assert_equal 123_456, value.usec
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
