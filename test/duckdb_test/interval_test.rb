require 'test_helper'

module DuckDBTest
  class IntervalTest < Minitest::Test
    def test_s_iso8601_parse
      assert_equal(
        DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 14_706_700_000),
        DuckDB::Interval.iso8601_parse('P1Y2M3DT4H5M6.7S')
      )
      assert_equal(
        DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 14_706_000_007),
        DuckDB::Interval.iso8601_parse('P1Y2M3DT4H5M6.000007S')
      )
      assert_equal(
        DuckDB::Interval.new(interval_months: 12),
        DuckDB::Interval.iso8601_parse('P1Y')
      )
      assert_equal(
        DuckDB::Interval.new(interval_months: 1),
        DuckDB::Interval.iso8601_parse('P1M')
      )
      assert_equal(
        DuckDB::Interval.new(interval_days: 1),
        DuckDB::Interval.iso8601_parse('P1D')
      )
      assert_equal(
        DuckDB::Interval.new(interval_micros: 3_600_000_000),
        DuckDB::Interval.iso8601_parse('PT1H')
      )
      assert_equal(
        DuckDB::Interval.new(interval_micros: 60_000_000),
        DuckDB::Interval.iso8601_parse('PT1M')
      )
      assert_equal(
        DuckDB::Interval.new(interval_micros: 1_000_000),
        DuckDB::Interval.iso8601_parse('PT1S')
      )
      assert_equal(
        DuckDB::Interval.new(interval_micros: 1),
        DuckDB::Interval.iso8601_parse('PT0.000001S')
      )
      assert_raises(ArgumentError) { DuckDB::Interval.iso8601_parse('1Y2M3DT4H5M6.7') }
    end

    def test_s_mk_interval
      assert_equal(
        DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 14_706_700_000),
        DuckDB::Interval.mk_interval(year: 1, month: 2, day: 3, hour: 4, min: 5, sec: 6, usec: 700_000)
      )
      assert_equal(
        DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 14_706_000_007),
        DuckDB::Interval.mk_interval(year: 1, month: 2, day: 3, hour: 4, min: 5, sec: 6, usec: 7)
      )
      assert_equal(
        DuckDB::Interval.new(interval_months: 12),
        DuckDB::Interval.mk_interval(year: 1)
      )
      assert_equal(
        DuckDB::Interval.new(interval_months: 1),
        DuckDB::Interval.mk_interval(month: 1)
      )
      assert_equal(
        DuckDB::Interval.new(interval_days: 1),
        DuckDB::Interval.mk_interval(day: 1)
      )
      assert_equal(
        DuckDB::Interval.new(interval_micros: 3_600_000_000),
        DuckDB::Interval.mk_interval(hour: 1)
      )
      assert_equal(
        DuckDB::Interval.new(interval_micros: 60_000_000),
        DuckDB::Interval.mk_interval(min: 1)
      )
      assert_equal(
        DuckDB::Interval.new(interval_micros: 1_000_000),
        DuckDB::Interval.mk_interval(sec: 1)
      )
      assert_equal(
        DuckDB::Interval.new(interval_micros: 1),
        DuckDB::Interval.mk_interval(usec: 1)
      )
    end

    def test_initialize
      assert_instance_of(DuckDB::Interval, DuckDB::Interval.new)
    end
  end
end
