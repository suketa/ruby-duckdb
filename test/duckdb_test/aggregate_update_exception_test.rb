# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  # The update callback converts each row value before handing it to the user's
  # proc, and only the proc call was protected. An exception from the conversion
  # therefore unwound into DuckDB's C++ frames and wedged the VM — reachable
  # from ordinary Ruby by wrapping a query in Timeout.timeout.
  class AggregateUpdateExceptionTest < Minitest::Test
    ROWS = 2000

    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      @con.query("CREATE TABLE d AS SELECT DATE '2020-01-01' + i::INTEGER AS d FROM range(#{ROWS}) s(i)")
      @con.register_aggregate_function(
        DuckDB::AggregateFunction.create(
          name: 'count_dates',
          return_type: :bigint,
          params: [:date],
          init: -> { 0 },
          update: ->(state, _v) { state + 1 },
          combine: ->(a, b) { a + b },
          finalize: ->(state) { state }
        )
      )
    end

    def teardown
      @con&.close
      @db&.close
    end

    # Stands in for anything that can raise between reading a row and calling the
    # user's proc; Timeout::Error lands in the same place, but not on a schedule.
    def with_conversion_raising(message)
      original = DuckDB::Converter.method(:_to_date)
      DuckDB::Converter.define_singleton_method(:_to_date) { |*| raise message }
      yield
    ensure
      DuckDB::Converter.define_singleton_method(:_to_date, original)
    end

    def failing_query
      with_conversion_raising('conversion blew up') do
        assert_raises(DuckDB::Error) { @con.query('SELECT count_dates(d) FROM d') }
      end
    end

    def test_a_conversion_error_aborts_the_query_as_a_duckdb_error
      assert_match(/conversion blew up/, failing_query.message)
    end

    def test_the_state_registry_drains_after_a_conversion_error
      baseline = DuckDB::AggregateFunction._state_registry_size

      failing_query

      assert_equal baseline, DuckDB::AggregateFunction._state_registry_size
    end

    def test_the_connection_still_works_after_a_conversion_error
      failing_query

      assert_equal [[ROWS]], @con.query('SELECT count_dates(d) FROM d').to_a
    end
  end
end
