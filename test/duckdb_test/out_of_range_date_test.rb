# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  # Dates and timestamps outside DuckDB's range must raise instead of
  # letting a C++ exception from duckdb_to_date/duckdb_to_timestamp abort the process.
  class OutOfRangeDateTest < Minitest::Test
    TOO_LARGE_DATE = Date.new(6_000_000, 1, 1)
    TOO_LARGE_TIMESTAMP = '300000-01-01 00:00:00'

    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
    end

    def teardown
      @con.close
      @db.close
    end

    def test_bind_date
      stmt = @con.prepare('SELECT ?::DATE')

      assert_raises(RangeError) { stmt.bind_date(1, TOO_LARGE_DATE) }
    end

    def test_bind_timestamp
      stmt = @con.prepare('SELECT ?::TIMESTAMP')

      assert_raises(RangeError) { stmt.bind_timestamp(1, TOO_LARGE_TIMESTAMP) }
    end

    def test_bind_timestamp_tz
      stmt = @con.prepare('SELECT ?::TIMESTAMPTZ')

      assert_raises(RangeError) { stmt.bind_timestamp_tz(1, TOO_LARGE_TIMESTAMP) }
    end

    def test_appender_append_date
      @con.query('CREATE TABLE t (d DATE)')
      appender = @con.appender('t')

      assert_raises(RangeError) { appender.append_date(TOO_LARGE_DATE) }
    end

    def test_appender_append_timestamp
      @con.query('CREATE TABLE t (ts TIMESTAMP)')
      appender = @con.appender('t')

      assert_raises(RangeError) { appender.append_timestamp(TOO_LARGE_TIMESTAMP) }
    end

    def test_value_create_date
      assert_raises(RangeError) { DuckDB::Value.create_date(TOO_LARGE_DATE) }
    end

    def test_value_create_timestamp
      assert_raises(RangeError) { DuckDB::Value.create_timestamp(TOO_LARGE_TIMESTAMP) }
    end

    def test_max_date_still_works
      stmt = @con.prepare('SELECT ?::DATE')
      stmt.bind_date(1, Date.new(5_881_580, 7, 10))

      assert_equal(Date.new(5_881_580, 7, 10), stmt.execute.first.first)
    end
  end
end
