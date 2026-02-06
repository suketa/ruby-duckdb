# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class IntervalTest < Minitest::Test
    def test_s_iso8601_parse_positive_full
      assert_equal(
        DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 14_706_700_000),
        DuckDB::Interval.iso8601_parse('P1Y2M3DT4H5M6.7S')
      )
      assert_equal(
        DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 14_706_000_007),
        DuckDB::Interval.iso8601_parse('P1Y2M3DT4H5M6.000007S')
      )
    end

    def test_s_iso8601_parse_negative_prefix
      assert_equal(
        DuckDB::Interval.new(interval_months: -14, interval_days: -3, interval_micros: -14_706_700_000),
        DuckDB::Interval.iso8601_parse('-P1Y2M3DT4H5M6.7S')
      )
      assert_equal(
        DuckDB::Interval.new(interval_months: -14, interval_days: -3, interval_micros: -14_706_000_007),
        DuckDB::Interval.iso8601_parse('-P1Y2M3DT4H5M6.000007S')
      )
    end

    def test_s_iso8601_parse_negative_components
      assert_equal(
        DuckDB::Interval.new(interval_months: -14, interval_days: -3, interval_micros: -14_706_700_000),
        DuckDB::Interval.iso8601_parse('P-1Y-2M-3DT-4H-5M-6.7S')
      )
      assert_equal(
        DuckDB::Interval.new(interval_months: -14, interval_days: -3, interval_micros: -14_706_000_007),
        DuckDB::Interval.iso8601_parse('P-1Y-2M-3DT-4H-5M-6.000007S')
      )
    end

    def test_s_iso8601_parse_single_units
      assert_equal(DuckDB::Interval.new(interval_months: 12), DuckDB::Interval.iso8601_parse('P1Y'))
      assert_equal(DuckDB::Interval.new(interval_months: 1), DuckDB::Interval.iso8601_parse('P1M'))
      assert_equal(DuckDB::Interval.new(interval_days: 1), DuckDB::Interval.iso8601_parse('P1D'))
    end

    def test_s_iso8601_parse_time_units
      assert_equal(DuckDB::Interval.new(interval_micros: 3_600_000_000), DuckDB::Interval.iso8601_parse('PT1H'))
      assert_equal(DuckDB::Interval.new(interval_micros: 60_000_000), DuckDB::Interval.iso8601_parse('PT1M'))
      assert_equal(DuckDB::Interval.new(interval_micros: 1_000_000), DuckDB::Interval.iso8601_parse('PT1S'))
    end

    def test_s_iso8601_parse_microseconds
      assert_equal(DuckDB::Interval.new(interval_micros: 1), DuckDB::Interval.iso8601_parse('PT0.000001S'))
    end

    def test_s_iso8601_parse_invalid
      assert_raises(ArgumentError) { DuckDB::Interval.iso8601_parse('1Y2M3DT4H5M6.7') }
    end

    def test_s_mk_interval_positive_full
      assert_equal(
        DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 14_706_700_000),
        DuckDB::Interval.mk_interval(year: 1, month: 2, day: 3, hour: 4, min: 5, sec: 6, usec: 700_000)
      )
      assert_equal(
        DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 14_706_000_007),
        DuckDB::Interval.mk_interval(year: 1, month: 2, day: 3, hour: 4, min: 5, sec: 6, usec: 7)
      )
    end

    def test_s_mk_interval_zero_and_empty
      assert_equal(
        DuckDB::Interval.new(interval_months: 0, interval_days: 0, interval_micros: 0),
        DuckDB::Interval.mk_interval(year: 0, month: 0, day: 0, hour: 0, min: 0, sec: 0, usec: 0)
      )
      assert_equal(DuckDB::Interval.new, DuckDB::Interval.mk_interval)
    end

    def test_s_mk_interval_negative_full
      assert_equal(
        DuckDB::Interval.new(interval_months: -14, interval_days: -3, interval_micros: -14_706_700_000),
        DuckDB::Interval.mk_interval(year: -1, month: -2, day: -3, hour: -4, min: -5, sec: -6, usec: -700_000)
      )
      assert_equal(
        DuckDB::Interval.new(interval_months: -14, interval_days: -3, interval_micros: -14_706_000_007),
        DuckDB::Interval.mk_interval(year: -1, month: -2, day: -3, hour: -4, min: -5, sec: -6, usec: -7)
      )
    end

    def test_s_mk_interval_single_date_units
      assert_equal(DuckDB::Interval.new(interval_months: 12), DuckDB::Interval.mk_interval(year: 1))
      assert_equal(DuckDB::Interval.new(interval_months: 1), DuckDB::Interval.mk_interval(month: 1))
      assert_equal(DuckDB::Interval.new(interval_days: 1), DuckDB::Interval.mk_interval(day: 1))
    end

    def test_s_mk_interval_single_time_units
      assert_equal(DuckDB::Interval.new(interval_micros: 3_600_000_000), DuckDB::Interval.mk_interval(hour: 1))
      assert_equal(DuckDB::Interval.new(interval_micros: 60_000_000), DuckDB::Interval.mk_interval(min: 1))
      assert_equal(DuckDB::Interval.new(interval_micros: 1_000_000), DuckDB::Interval.mk_interval(sec: 1))
    end

    def test_s_mk_interval_microseconds
      assert_equal(DuckDB::Interval.new(interval_micros: 1), DuckDB::Interval.mk_interval(usec: 1))
    end

    def test_initialize
      interval = DuckDB::Interval.new

      assert_instance_of(DuckDB::Interval, interval)
    end

    def test_initialize_default_values
      interval = DuckDB::Interval.new

      assert_equal(0, interval.interval_months)
      assert_equal(0, interval.interval_days)
      assert_equal(0, interval.interval_micros)
    end

    def test_equality
      interval1 = DuckDB::Interval.new
      interval2 = DuckDB::Interval.new

      assert_equal(interval1, interval2)
    end

    def test_eql?
      interval1 = DuckDB::Interval.new
      interval2 = DuckDB::Interval.new

      assert(interval1.eql?(interval2))
    end
  end
end
