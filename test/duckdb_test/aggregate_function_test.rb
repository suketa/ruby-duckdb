# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class AggregateFunctionTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
    end

    def teardown
      @con&.close
      @db&.close
    end

    def test_minimal_aggregate_returns_initial_state
      af = build_aggregate('my_agg',
                           init: -> { 42 },
                           finalize: ->(state) { state })
      @con.register_aggregate_function(af)

      result = @con.query('SELECT my_agg(i) FROM range(100) t(i)')

      assert_equal 42, result.first.first
    end

    def test_aggregate_update_sums_values
      af = build_aggregate('my_sum',
                           init: -> { 0 },
                           update: ->(state, value) { state + value },
                           finalize: ->(state) { state })
      @con.register_aggregate_function(af)

      result = @con.query('SELECT my_sum(i) FROM range(100) t(i)')

      # sum(0..99) == 4950
      assert_equal 4950, result.first.first
    end

    def test_aggregate_double_return_and_input
      double_type = DuckDB::LogicalType::DOUBLE
      af = build_aggregate('my_dsum',
                           type: double_type,
                           init: -> { 0.0 },
                           update: ->(state, value) { state + value },
                           combine: ->(s1, s2) { s1 + s2 },
                           finalize: ->(state) { state })
      @con.register_aggregate_function(af)

      result = @con.query('SELECT my_dsum(i::DOUBLE) FROM range(10) t(i)')
      # sum(0.0..9.0) == 45.0
      assert_in_delta 45.0, result.first.first, 0.001
    end

    def test_aggregate_varchar_return_and_input
      varchar_type = DuckDB::LogicalType::VARCHAR
      af = build_aggregate('my_concat',
                           type: varchar_type,
                           init: -> { +'' },
                           update: ->(state, value) { state + value },
                           combine: ->(s1, s2) { s1 + s2 },
                           finalize: ->(state) { state })
      @con.register_aggregate_function(af)

      result = @con.query("SELECT my_concat(x) FROM (VALUES ('a'), ('b'), ('c')) t(x)")

      assert_equal 'abc', result.first.first
    end

    def test_aggregate_destructor_cleans_up_states_after_error
      # Register an aggregate whose update block deliberately raises for a
      # specific input value so the query fails before finalize is called.
      af = build_aggregate('error_sum',
                           init: -> { 0 },
                           update: ->(state, value) {
                             raise 'deliberate error' if value == 50
                             state + value
                           },
                           finalize: ->(state) { state })
      @con.register_aggregate_function(af)

      # The query must fail — the update block raises for input 50.
      assert_raises(DuckDB::Error) do
        @con.query('SELECT error_sum(i) FROM range(100) t(i)')
      end

      # Apply GC pressure.  Without a destructor registered via
      # duckdb_aggregate_function_set_destructor the g_aggregate_state_registry
      # still holds entries for every state DuckDB freed without calling
      # finalize — those entries will never be removed.
      GC.start
      GC.compact if GC.respond_to?(:compact)
      GC.start

      # With the destructor wired the registry must be empty now.
      # _state_registry_size is a C-level helper exposed specifically to make
      # this invariant observable from Ruby tests.
      assert_equal 0, DuckDB::AggregateFunction._state_registry_size,
                   'state registry must be empty after all aggregate states are destroyed'
    end

    def test_aggregate_combine_merges_partial_states_in_parallel
      af = build_aggregate('my_parallel_sum',
                           init: -> { 0 },
                           update: ->(state, value) { state + value },
                           combine: ->(s1, s2) { s1 + s2 },
                           finalize: ->(state) { state })
      @con.register_aggregate_function(af)
      force_parallel_execution(@con)

      result = @con.query('SELECT my_parallel_sum(i) FROM range(100000) t(i)')
      # sum(0..99_999) == 4_999_950_000
      assert_equal 4_999_950_000, result.first.first
    end

    private

    # Force DuckDB to actually parallelise aggregation so the combine callback
    # receives more than one partial state to merge.
    def force_parallel_execution(con)
      con.query('PRAGMA threads=4')
      con.query('PRAGMA verify_parallelism')
    end

    def build_aggregate(name, type: DuckDB::LogicalType::BIGINT, **callbacks)
      af = DuckDB::AggregateFunction.new
      af.name = name
      af.return_type = type
      af.add_parameter(type)
      set_callbacks(af, callbacks)
      af
    end

    def set_callbacks(func, callbacks)
      func.set_init(&callbacks[:init])
      func.set_update(&callbacks[:update]) if callbacks[:update]
      func.set_combine(&callbacks[:combine]) if callbacks[:combine]
      func.set_finalize(&callbacks[:finalize])
    end
  end
end
