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
      af = DuckDB::AggregateFunction.new
      af.name = 'my_agg'
      af.return_type = DuckDB::LogicalType::BIGINT
      af.add_parameter(DuckDB::LogicalType::BIGINT)
      af.set_init     { 42 }
      af.set_finalize { |state| state }

      @con.register_aggregate_function(af)

      result = @con.query('SELECT my_agg(i) FROM range(100) t(i)')

      assert_equal 42, result.first.first
    end

    def test_aggregate_update_sums_values
      af = DuckDB::AggregateFunction.new
      af.name = 'my_sum'
      af.return_type = DuckDB::LogicalType::BIGINT
      af.add_parameter(DuckDB::LogicalType::BIGINT)
      af.set_init     { 0 }
      af.set_update   { |state, value| state + value }
      af.set_finalize { |state| state }

      @con.register_aggregate_function(af)

      result = @con.query('SELECT my_sum(i) FROM range(100) t(i)')

      # sum(0..99) == 4950
      assert_equal 4950, result.first.first
    end
  end
end
