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
      register_aggregate('my_agg',
                         init: -> { 42 },
                         finalize: ->(state) { state })

      result = @con.query('SELECT my_agg(i) FROM range(100) t(i)')

      assert_equal 42, result.first.first
    end

    def test_aggregate_update_sums_values
      register_aggregate('my_sum',
                         init: -> { 0 },
                         update: ->(state, value) { state + value },
                         finalize: ->(state) { state })

      result = @con.query('SELECT my_sum(i) FROM range(100) t(i)')

      # sum(0..99) == 4950
      assert_equal 4950, result.first.first
    end

    def test_aggregate_double_return_and_input
      double_type = DuckDB::LogicalType::DOUBLE
      register_aggregate('my_dsum',
                         type: double_type,
                         init: -> { 0.0 },
                         update: ->(state, value) { state + value },
                         combine: ->(s1, s2) { s1 + s2 },
                         finalize: ->(state) { state })

      result = @con.query('SELECT my_dsum(i::DOUBLE) FROM range(10) t(i)')
      # sum(0.0..9.0) == 45.0
      assert_in_delta 45.0, result.first.first, 0.001
    end

    def test_aggregate_varchar_return_and_input
      varchar_type = DuckDB::LogicalType::VARCHAR
      register_aggregate('my_concat',
                         type: varchar_type,
                         init: -> { +'' },
                         update: ->(state, value) { state + value },
                         combine: ->(s1, s2) { s1 + s2 },
                         finalize: ->(state) { state })

      result = @con.query("SELECT my_concat(x) FROM (VALUES ('a'), ('b'), ('c')) t(x)")

      assert_equal 'abc', result.first.first
    end

    def test_aggregate_destructor_cleans_up_states_after_successful_query
      # Record baseline — previous tests may have left entries in the
      # global registry (the registry is process-global).
      baseline = DuckDB::AggregateFunction._state_registry_size

      # Register and run a normal aggregate query that succeeds.
      register_aggregate('cleanup_sum',
                         init: -> { 0 },
                         update: ->(state, value) { state + value },
                         finalize: ->(state) { state })

      result = @con.query('SELECT cleanup_sum(i) FROM range(100) t(i)')

      assert_equal 4950, result.first.first

      # With the destructor wired via duckdb_aggregate_function_set_destructor,
      # all states allocated during the query are cleaned up through either
      # finalize (for the final state) or destroy (for intermediate states
      # that DuckDB memcpy'd internally).  The registry must return to
      # baseline.
      assert_equal baseline, DuckDB::AggregateFunction._state_registry_size,
                   'state registry must not grow after a successful aggregate query'
    end

    def test_aggregate_state_cleanup_after_finalize_error
      baseline = DuckDB::AggregateFunction._state_registry_size

      register_aggregate('err_finalize',
                         init: -> { 0 },
                         update: ->(state, value) { state + value },
                         finalize: ->(_state) { raise 'finalize boom' })

      assert_raises(DuckDB::Error) do
        @con.query('SELECT err_finalize(i) FROM range(10) t(i)')
      end

      assert_equal baseline, DuckDB::AggregateFunction._state_registry_size,
                   'state registry must not leak after a finalize callback error'
    end

    def test_aggregate_update_error_surfaces_without_registry_leak
      # When the update block raises, the error is surfaced as DuckDB::Error.
      # DuckDB does NOT call the destroy callback on the update error path,
      # so we clean up reachable states ourselves in the update error handler.
      baseline = DuckDB::AggregateFunction._state_registry_size
      update_proc = lambda { |state, value|
        raise 'boom in update' if value == 5

        state + value
      }
      register_aggregate('err_update', init: -> { 0 }, update: update_proc, finalize: ->(state) { state })

      error = assert_raises(DuckDB::Error) { @con.query('SELECT err_update(i) FROM range(10) t(i)') }

      assert_match(/boom in update/, error.message)
      assert_equal baseline, DuckDB::AggregateFunction._state_registry_size,
                   'state registry must not leak after an update callback error'
    end

    def test_aggregate_combine_merges_partial_states_in_parallel
      register_aggregate('my_parallel_sum',
                         init: -> { 0 },
                         update: ->(state, value) { state + value },
                         combine: ->(s1, s2) { s1 + s2 },
                         finalize: ->(state) { state })
      force_parallel_execution(@con)

      result = @con.query('SELECT my_parallel_sum(i) FROM range(100000) t(i)')
      # sum(0..99_999) == 4_999_950_000
      assert_equal 4_999_950_000, result.first.first
    end

    def test_gc_compaction_safety
      skip 'GC.compact not available' unless GC.respond_to?(:compact)

      baseline = DuckDB::AggregateFunction._state_registry_size

      # Register an aggregate whose Proc objects are stored as Ruby VALUEs inside
      # the C extension struct.  compact_heap moves every moveable object; any
      # VALUE NOT updated via rb_gc_location in the compact callback becomes a
      # stale pointer and the subsequent query will crash or return wrong results.
      register_aggregate(
        'gc_compact_sum',
        init: -> { 0 },
        update: ->(state, value) { state + value },
        combine: ->(s1, s2) { s1 + s2 },
        finalize: ->(state) { state }
      )

      compact_heap

      result = @con.query('SELECT gc_compact_sum(i) FROM range(100) t(i)')

      # sum(0..99) == 4950
      assert_equal 4950, result.first.first,
                   'Aggregate callback returned wrong result after GC compaction'
      assert_equal baseline, DuckDB::AggregateFunction._state_registry_size,
                   'State registry must return to baseline after a successful query post-compaction'

      # A second compaction after the query must also be safe (no dangling refs).
      compact_heap

      result2 = @con.query('SELECT gc_compact_sum(i) FROM range(10) t(i)')

      # sum(0..9) == 45
      assert_equal 45, result2.first.first,
                   'Aggregate callback returned wrong result after second GC compaction'
    end

    def test_set_special_handling_passes_null_values_to_update
      af = build_aggregate('count_with_nulls',
                           init: -> { 0 },
                           update: ->(state, _value) { state + 1 },
                           finalize: ->(state) { state })
      af.set_special_handling
      @con.register_aggregate_function(af)

      @con.query('CREATE TABLE null_test (i BIGINT)')
      @con.query('INSERT INTO null_test VALUES (1), (NULL), (3), (NULL), (5)')

      result = @con.query('SELECT count_with_nulls(i) FROM null_test')

      # Without set_special_handling DuckDB would skip the 2 NULL rows and
      # return 3.  With it the update callback receives all 5 rows (NULLs
      # arrive as nil), so the count must be 5.
      assert_equal 5, result.first.first
    end

    def test_aggregate_with_hash_state
      double_type = DuckDB::LogicalType::DOUBLE

      af = DuckDB::AggregateFunction.new
      af.name = 'my_avg'
      af.return_type = double_type
      af.add_parameter(double_type)
      af.set_init { { sum: 0.0, count: 0 } }
      # set_update modifies the Hash state in place and returns it.
      # The returned VALUE must replace the state entry in the registry so
      # that subsequent callbacks (combine, finalize) receive the up-to-date
      # Hash rather than the init state. This exercises the full round-trip of
      # a complex Ruby heap object through the state registry.
      af.set_update { |state, value| state[:sum] += value; state[:count] += 1; state }
      af.set_combine { |s1, s2| { sum: s1[:sum] + s2[:sum], count: s1[:count] + s2[:count] } }
      af.set_finalize { |state| state[:count] > 0 ? state[:sum] / state[:count] : nil }
      @con.register_aggregate_function(af)

      result = @con.query('SELECT my_avg(i::DOUBLE) FROM range(11) t(i)')

      # average of 0..10 == 5.0
      assert_in_delta 5.0, result.first.first, 0.001
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

    def register_aggregate(name, **)
      af = build_aggregate(name, **)
      @con.register_aggregate_function(af)
    end

    # Use verify_compaction_references when available — it double-compacts the
    # heap, moving every moveable object at least twice, which is stricter than
    # a single GC.compact pass.
    def compact_heap
      if GC.respond_to?(:verify_compaction_references)
        GC.verify_compaction_references(double_heap: true, toward: :empty)
      else
        GC.compact
      end
    end

    def set_callbacks(func, callbacks)
      func.set_init(&callbacks[:init])
      func.set_update(&callbacks[:update]) if callbacks[:update]
      func.set_combine(&callbacks[:combine]) if callbacks[:combine]
      func.set_finalize(&callbacks[:finalize])
    end
  end
end
