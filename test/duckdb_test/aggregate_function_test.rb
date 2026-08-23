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

    def test_s_create
      af = DuckDB::AggregateFunction.create(
        name: 'my_sum',
        return_type: :bigint,
        params: [:bigint],
        init: -> { 0 },
        update: ->(state, value) { state + value },
        combine: ->(state, other_state) { state + other_state }
      )
      @con.register_aggregate_function(af)
      result = @con.query('SELECT my_sum(i) FROM range(100) t(i)')

      assert_equal 4950, result.first.first
    end

    def test_time_ns_parameter_and_return_type
      # The C API supports TIME_NS fully on DuckDB >= 1.5.0.
      skip 'TIME_NS requires DuckDB >= 1.5.0' if ::DuckDBTest.duckdb_library_version < Gem::Version.new('1.5.0')

      af = DuckDB::AggregateFunction.create(
        name: 'my_max_time_ns',
        return_type: :time_ns,
        params: [:time_ns],
        init: -> {},
        update: ->(state, value) { state && state > value ? state : value },
        combine: ->(state, other_state) { [state, other_state].compact.max }
      )
      @con.register_aggregate_function(af)
      @con.execute('CREATE TABLE test_table (t TIME_NS)')
      @con.execute("INSERT INTO test_table VALUES ('10:30:00.123456789'), ('23:59:59.999999999')")

      result = @con.query('SELECT my_max_time_ns(t) FROM test_table').first.first

      assert_equal [23, 59, 59, 999_999_999], [result.hour, result.min, result.sec, result.nsec]
    end

    def test_default_finalize_returns_state_as_is
      register_aggregate('my_agg_default_finalize',
                         init: -> { 42 })

      result = @con.query('SELECT my_agg_default_finalize(i) FROM range(100) t(i)')

      assert_equal 42, result.first.first
    end

    def test_default_combine_single_threaded_correctness
      # Runs single-threaded so DuckDB uses one partition and never calls
      # combine. The default combine { |s1, _s2| s1 } is lossy under parallel
      # execution (discards the target partition), so correctness can only be
      # asserted in the single-partition case.
      register_aggregate('my_agg_default_combine',
                         init: -> { 0 },
                         update: ->(state, value) { state + value })

      result = @con.query('SELECT my_agg_default_combine(i) FROM range(100) t(i)')

      assert_equal 4950, result.first.first
    end

    def test_s_create_zero_param_aggregate
      af = DuckDB::AggregateFunction.create(
        name: 'my_count',
        return_type: :bigint,
        init: -> { 0 },
        update: ->(state, *) { state + 1 },
        combine: ->(state, other_state) { state + other_state }
      )
      @con.register_aggregate_function(af)
      result = @con.query('SELECT my_count() FROM range(5) t(i)')

      assert_equal 5, result.first.first
    end

    def test_s_create_null_handling_true
      af = DuckDB::AggregateFunction.create(
        name: 'count_with_nulls',
        return_type: :bigint,
        params: [:bigint],
        init: -> { 0 },
        update: ->(state, _value) { state + 1 },
        combine: ->(state, other_state) { state + other_state },
        null_handling: true
      )
      @con.register_aggregate_function(af)
      @con.query('CREATE TABLE null_handling_test (i BIGINT)')
      @con.query('INSERT INTO null_handling_test VALUES (1), (NULL), (3), (NULL), (5)')

      result = @con.query('SELECT count_with_nulls(i) FROM null_handling_test')

      assert_equal 5, result.first.first
    end

    def test_s_create_raises_argument_error_for_non_callable_init
      assert_raises(ArgumentError) do
        DuckDB::AggregateFunction.create(
          name: 'bad_init',
          return_type: :bigint,
          init: 0
        )
      end
    end

    def test_set_update_after_set_init_overrides_default
      af = DuckDB::AggregateFunction.new
      af.name = 'sum_update_after_init'
      af.return_type = DuckDB::LogicalType::BIGINT
      af.add_parameter(DuckDB::LogicalType::BIGINT)
      af.set_init { 0 }
      af.set_update { |state, value| state + value }
      @con.register_aggregate_function(af)

      result = @con.query('SELECT sum_update_after_init(i) FROM range(100) t(i)')

      # default update would ignore inputs and return 0; real update sums them
      assert_equal 4950, result.first.first
    end

    def test_set_combine_after_set_init_overrides_default
      af = DuckDB::AggregateFunction.new
      af.name = 'sum_combine_after_init'
      af.return_type = DuckDB::LogicalType::BIGINT
      af.add_parameter(DuckDB::LogicalType::BIGINT)
      af.set_init { 0 }
      af.set_update { |state, value| state + value }
      af.set_combine { |s1, s2| s1 + s2 }
      @con.register_aggregate_function(af)
      force_parallel_execution(@con)

      result = @con.query('SELECT sum_combine_after_init(i) FROM range(100000) t(i)')

      assert_equal 4_999_950_000, result.first.first
    end

    def test_set_finalize_after_set_init_overrides_default
      af = DuckDB::AggregateFunction.new
      af.name = 'sum_finalize_after_init'
      af.return_type = DuckDB::LogicalType::BIGINT
      af.add_parameter(DuckDB::LogicalType::BIGINT)
      af.set_init { 0 }
      af.set_update { |state, value| state + value }
      af.set_finalize { |state| state * 2 }
      @con.register_aggregate_function(af)

      result = @con.query('SELECT sum_finalize_after_init(i) FROM range(100) t(i)')

      # sum(0..99) == 4950, doubled by finalize == 9900
      assert_equal 9900, result.first.first
    end

    def test_set_init_called_twice_second_block_wins
      af = DuckDB::AggregateFunction.new
      af.name = 'init_twice'
      af.return_type = DuckDB::LogicalType::BIGINT
      af.add_parameter(DuckDB::LogicalType::BIGINT)
      af.set_init { 100 }
      af.set_init { 0 }
      af.set_update { |state, value| state + value }
      @con.register_aggregate_function(af)

      result = @con.query('SELECT init_twice(i) FROM range(100) t(i)')

      # initial state is 0 (second set_init wins), so sum == 4950
      assert_equal 4950, result.first.first
    end

    def test_bypass_ruby_wrapper_raises_on_finalize
      # Calling _set_init directly skips the Ruby wrapper's default injection,
      # leaving finalize_proc as Qnil. The C nil-guard must surface a
      # DuckDB::Error instead of crashing with SIGSEGV.
      af = DuckDB::AggregateFunction.new
      af.name = 'bypass_init_finalize'
      af.return_type = DuckDB::LogicalType::BIGINT
      af.add_parameter(DuckDB::LogicalType::BIGINT)
      af.send(:_set_init) { 0 }
      @con.register_aggregate_function(af)

      assert_raises(DuckDB::Error) do
        @con.query('SELECT bypass_init_finalize(i) FROM range(10) t(i)')
      end
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
      update_proc = lambda { |state, value|
        state[:sum] += value
        state[:count] += 1
        state
      }
      register_aggregate(
        'my_avg',
        type: double_type,
        init: -> { { sum: 0.0, count: 0 } },
        update: update_proc,
        combine: ->(s1, s2) { { sum: s1[:sum] + s2[:sum], count: s1[:count] + s2[:count] } },
        finalize: ->(state) { state[:count].positive? ? state[:sum] / state[:count] : nil }
      )

      result = @con.query('SELECT my_avg(i::DOUBLE) FROM range(11) t(i)')

      # average of 0..10 == 5.0
      assert_in_delta 5.0, result.first.first, 0.001
    end

    def test_return_type_setter_raises_for_unsupported_type
      af = DuckDB::AggregateFunction.new
      bit_type = DuckDB::LogicalType::BIT # BIT is unsupported as a function return type

      assert_raises(DuckDB::Error) do
        af.return_type = bit_type
      end
    end

    def test_add_parameter_raises_for_unsupported_type
      af = DuckDB::AggregateFunction.new
      bit_type = DuckDB::LogicalType::BIT # BIT is unsupported as a parameter type

      assert_raises(DuckDB::Error) do
        af.add_parameter(bit_type)
      end
    end

    def test_null_values_skipped_by_default
      # Without set_special_handling, DuckDB's default behaviour is to skip
      # NULL input values before calling the update callback.
      register_aggregate('count_skip_nulls',
                         init: -> { 0 },
                         update: ->(state, _value) { state + 1 },
                         finalize: ->(state) { state })

      @con.query('CREATE TABLE null_skip_test (i BIGINT)')
      @con.query('INSERT INTO null_skip_test VALUES (1), (NULL), (3), (NULL), (5)')

      result = @con.query('SELECT count_skip_nulls(i) FROM null_skip_test')

      # The 2 NULL rows must be skipped; only the 3 non-NULL rows reach update.
      assert_equal 3, result.first.first
    end

    def test_aggregate_with_multiple_parameters
      bigint = DuckDB::LogicalType::BIGINT
      af = DuckDB::AggregateFunction.new
      af.name = 'weighted_sum'
      af.return_type = bigint
      af.add_parameter(bigint) # value
      af.add_parameter(bigint) # weight
      set_callbacks(af,
                    init: -> { 0 },
                    update: ->(state, value, weight) { state + (value * weight) },
                    combine: ->(s1, s2) { s1 + s2 },
                    finalize: ->(state) { state })
      @con.register_aggregate_function(af)

      @con.query('CREATE TABLE weighted (v BIGINT, w BIGINT)')
      @con.query('INSERT INTO weighted VALUES (1, 2), (3, 4), (5, 6)')

      result = @con.query('SELECT weighted_sum(v, w) FROM weighted')

      # 1*2 + 3*4 + 5*6 = 2 + 12 + 30 = 44
      assert_equal 44, result.first.first
    end

    def test_name_setter_accepts_symbol
      af = DuckDB::AggregateFunction.new
      result = af.name = :my_agg

      assert_instance_of DuckDB::AggregateFunction, af
      assert_equal 'my_agg', result.to_s
    end

    # A window frame feeds the same source state into many combine calls, so
    # anything that releases the source state after the first call loses it for
    # every later one.
    def test_aggregate_as_window_function_reuses_source_state
      force_parallel_execution(@con)
      @con.query('CREATE TABLE w AS SELECT i FROM generate_series(1, 5000) s(i)')
      nil_states = 0

      @con.register_aggregate_function(
        DuckDB::AggregateFunction.create(
          name: 'win_count',
          return_type: :bigint,
          params: [:bigint],
          init: -> { 0 },
          update: ->(state, _value) { state + 1 },
          combine: lambda { |state, other_state|
            nil_states += 1 if state.nil? || other_state.nil?
            (state || 0) + (other_state || 0)
          },
          finalize: ->(state) { state }
        )
      )

      rows = @con.query(
        'SELECT win_count(i) OVER (ORDER BY i ROWS BETWEEN 100 PRECEDING AND 100 FOLLOWING) FROM w'
      ).to_a

      assert_equal 0, nil_states
      assert_equal [[101], [201], [101]], [rows.first, rows[2500], rows.last]
    end

    # An empty OVER () frame is constant over the whole partition, so DuckDB
    # switches to WindowConstantAggregator, which builds the states array as a
    # constant vector: a single eight-byte allocation holding the partition's
    # one state, passed along with the partition's full row count. Reading one
    # state per row ran off the end of it and dereferenced whatever followed.
    def test_aggregate_over_an_empty_window_counts_the_whole_partition
      @con.register_aggregate_function(
        DuckDB::AggregateFunction.create(
          name: 'const_count',
          return_type: :bigint,
          params: [:bigint],
          init: -> { 0 },
          update: ->(state, _value) { state + 1 },
          combine: ->(state, other_state) { (state || 0) + (other_state || 0) },
          finalize: ->(state) { state }
        )
      )

      # More than one row count: only the first entry of that array is ever the
      # real state, whatever the partition size, so a fix that trusts a fixed
      # prefix passes at tiny row counts and is silently wrong at real ones.
      [2, 100, 5000].each do |row_count|
        @con.query("CREATE OR REPLACE TABLE c AS SELECT i FROM generate_series(1, #{row_count}) s(i)")

        rows = @con.query('SELECT const_count(i) OVER () FROM c').to_a

        assert_equal row_count, rows.size, 'the window should emit one row per input row'
        assert_equal [[row_count]], rows.uniq, "every row should see all #{row_count} rows"
      end
    end

    # PARTITION BY keeps the frame constant but gives DuckDB several partition
    # states at once, so the bytes past the states array can hold a live state
    # belonging to a *different* partition. Aggregating a row into that one
    # would be silently wrong rather than a crash, which is what makes this
    # worth covering separately from the single-partition OVER () case.
    def test_aggregate_over_a_partitioned_constant_window_counts_each_partition
      @con.register_aggregate_function(
        DuckDB::AggregateFunction.create(
          name: 'part_count',
          return_type: :bigint,
          params: [:bigint],
          init: -> { 0 },
          update: ->(state, _value) { state + 1 },
          combine: ->(state, other_state) { (state || 0) + (other_state || 0) },
          finalize: ->(state) { state }
        )
      )

      @con.query('CREATE TABLE p AS SELECT i, i % 4 AS g FROM generate_series(1, 4000) s(i)')

      rows = @con.query('SELECT g, part_count(i) OVER (PARTITION BY g) FROM p').to_a

      assert_equal 4000, rows.size
      assert_equal [[0, 1000], [1, 1000], [2, 1000], [3, 1000]], rows.uniq.sort
    end

    # Failing mid-chunk under the constant layout used to release every entry of
    # the states array, and the bytes past that array can hold live states of
    # other partitions -- so the cleanup dropped registry entries the chunk had
    # never touched. The damage only shows up afterwards, hence the queries at
    # the end rather than an assertion on the failure itself.
    def test_a_failed_update_over_a_constant_window_leaves_later_queries_intact
      calls = 0
      raising = false

      @con.register_aggregate_function(
        DuckDB::AggregateFunction.create(
          name: 'flaky_count',
          return_type: :bigint,
          params: [:bigint],
          init: -> { 0 },
          update: lambda { |state, _value|
            calls += 1
            raise 'update failed' if raising && calls > 3000

            state + 1
          },
          combine: ->(state, other_state) { (state || 0) + (other_state || 0) },
          finalize: ->(state) { state }
        )
      )

      @con.query('CREATE TABLE f AS SELECT i, i % 8 AS g FROM generate_series(1, 8000) s(i)')
      baseline = DuckDB::AggregateFunction._state_registry_size

      raising = true
      assert_raises(DuckDB::Error) do
        @con.query('SELECT g, flaky_count(i) OVER (PARTITION BY g) FROM f').to_a
      end
      raising = false

      assert_equal baseline, DuckDB::AggregateFunction._state_registry_size,
                   'the failed query must not leave registry entries behind'

      rows = @con.query('SELECT g, flaky_count(i) OVER (PARTITION BY g) FROM f').to_a

      assert_equal [[0, 1000], [1, 1000], [2, 1000], [3, 1000],
                    [4, 1000], [5, 1000], [6, 1000], [7, 1000]], rows.uniq.sort,
                   'a state released by the failed query would resurface as a wrong count here'
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
      func.set_finalize(&callbacks[:finalize]) if callbacks[:finalize]
    end
  end
end
