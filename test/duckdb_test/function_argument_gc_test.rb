# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  # Per-row arguments are built one column at a time, and each conversion can
  # allocate. Under GC.stress every allocation collects, so an argument that is
  # not reachable from a scanned root is gone before the block runs.
  class FunctionArgumentGCTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      @con.query('CREATE TABLE t (a VARCHAR, b VARCHAR, c VARCHAR)')
      @con.query("INSERT INTO t VALUES ('alpha', 'beta', 'gamma'), ('delta', 'epsilon', 'zeta')")
    end

    def teardown
      GC.stress = false
      @con&.close
      @db&.close
    end

    def with_gc_stress
      GC.stress = true
      yield
    ensure
      GC.stress = false
    end

    def test_scalar_function_arguments_survive_gc_between_columns
      @con.register_scalar_function(DuckDB::ScalarFunction.new.tap do |sf|
        sf.name = 'join3'
        3.times { sf.add_parameter(DuckDB::LogicalType::VARCHAR) }
        sf.return_type = DuckDB::LogicalType::VARCHAR
        sf.set_function { |a, b, c| [a, b, c].join('-') }
      end)

      result = with_gc_stress { @con.query('SELECT join3(a, b, c) FROM t ORDER BY a').to_a }

      assert_equal [['alpha-beta-gamma'], ['delta-epsilon-zeta']], result
    end

    def test_aggregate_function_arguments_survive_gc_between_columns
      @con.register_aggregate_function(
        DuckDB::AggregateFunction.create(
          name: 'concat_pairs',
          return_type: :varchar,
          params: %i[varchar varchar],
          init: -> { [] },
          update: ->(state, a, b) { state + ["#{a}:#{b}"] },
          combine: ->(state, other_state) { state + other_state },
          finalize: ->(state) { state.sort.join(',') }
        )
      )

      result = with_gc_stress { @con.query('SELECT concat_pairs(a, b) FROM t').first.first }

      assert_equal 'alpha:beta,delta:epsilon', result
    end

    def test_aggregate_function_state_survives_compaction
      skip 'GC.compact not available' unless GC.respond_to?(:compact)
      skip 'GC.compact hangs on Windows in parallel test execution' if Gem.win_platform?

      @con.query('CREATE TABLE g AS SELECT (i % 40)::VARCHAR AS k, i::VARCHAR AS v ' \
                 'FROM generate_series(1, 400) t(i)')

      @con.register_aggregate_function(
        DuckDB::AggregateFunction.create(
          name: 'compact_count',
          return_type: :varchar,
          params: %i[varchar],
          init: -> { [] },
          update: lambda { |state, v|
            # Churn, then compact, so the state of every group that is not the
            # one being updated right now gets relocated behind its back.
            10.times { +'garbage' * 20 }
            GC.compact
            state + [v]
          },
          combine: ->(state, other_state) { state + other_state },
          finalize: ->(state) { state.size.to_s }
        )
      )

      result = @con.query('SELECT compact_count(v) FROM g GROUP BY k').to_a

      assert_equal 40, result.size
      assert_equal [['10']], result.uniq
    end

    def test_scalar_function_arguments_survive_compaction
      skip 'GC.compact not available' unless GC.respond_to?(:compact)
      skip 'GC.compact hangs on Windows in parallel test execution' if Gem.win_platform?

      @con.register_scalar_function(DuckDB::ScalarFunction.new.tap do |sf|
        sf.name = 'first_of_two'
        2.times { sf.add_parameter(DuckDB::LogicalType::VARCHAR) }
        sf.return_type = DuckDB::LogicalType::VARCHAR
        sf.set_function do |a, b|
          GC.compact
          "#{a}/#{b}"
        end
      end)

      result = @con.query('SELECT first_of_two(a, b) FROM t ORDER BY a').to_a

      assert_equal [['alpha/beta'], ['delta/epsilon']], result
    end
  end
end
