# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ValueTemporalTest < Minitest::Test
    def test_create_date_with_date
      value = DuckDB::Value.create_date(Date.new(2026, 7, 12))

      assert_instance_of(DuckDB::Value, value)
      assert_equal(Date.new(2026, 7, 12), value.to_ruby)
    end

    def test_create_date_with_string
      assert_equal(Date.new(2026, 7, 12), DuckDB::Value.create_date('2026-07-12').to_ruby)
    end

    def test_create_date_with_time
      time = Time.local(2026, 7, 12, 1, 2, 3)

      assert_equal(Date.new(2026, 7, 12), DuckDB::Value.create_date(time).to_ruby)
    end

    def test_create_time_with_time
      time = Time.local(2026, 7, 12, 12, 34, 56, 789_012)
      value = DuckDB::Value.create_time(time)

      assert_instance_of(DuckDB::Value, value)
      result = value.to_ruby

      assert_equal([12, 34, 56, 789_012], [result.hour, result.min, result.sec, result.usec])
    end

    def test_create_time_with_string
      result = DuckDB::Value.create_time('12:34:56.789').to_ruby

      assert_equal([12, 34, 56, 789_000], [result.hour, result.min, result.sec, result.usec])
    end

    def test_create_time_with_invalid_string_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_time('not a time') }
    end

    def test_create_time_with_nil_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_time(nil) }
    end

    def test_create_time_ns_with_time_preserves_nanoseconds
      # Value#to_ruby cannot convert TIME_NS on DuckDB < 1.5.0.
      skip 'TIME_NS requires DuckDB >= 1.5.0' if ::DuckDBTest.duckdb_library_version < Gem::Version.new('1.5.0')

      time = Time.local(2026, 7, 12, 12, 34, 56, Rational(123_456_789, 1000))
      value = DuckDB::Value.create_time_ns(time)

      assert_instance_of(DuckDB::Value, value)
      result = value.to_ruby

      assert_equal([12, 34, 56, 123_456_789], [result.hour, result.min, result.sec, result.nsec])
    end

    def test_create_time_ns_with_string
      # Value#to_ruby cannot convert TIME_NS on DuckDB < 1.5.0.
      skip 'TIME_NS requires DuckDB >= 1.5.0' if ::DuckDBTest.duckdb_library_version < Gem::Version.new('1.5.0')

      result = DuckDB::Value.create_time_ns('12:34:56.123456789').to_ruby

      assert_equal(123_456_789, result.nsec)
    end

    def test_create_time_ns_with_invalid_string_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_time_ns('not a time') }
    end

    def test_create_time_ns_with_nil_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_time_ns(nil) }
    end

    def test_create_time_tz_with_time
      time = Time.new(2026, 7, 12, 12, 34, 56, '+05:30')
      value = DuckDB::Value.create_time_tz(time)

      assert_instance_of(DuckDB::Value, value)
      result = value.to_ruby

      assert_equal([12, 34, 56, 19_800], [result.hour, result.min, result.sec, result.utc_offset])
    end

    def test_create_time_tz_with_string
      result = DuckDB::Value.create_time_tz('12:34:56.789012-08:00').to_ruby

      assert_equal([12, 34, 56, 789_012, -28_800],
                   [result.hour, result.min, result.sec, result.usec, result.utc_offset])
    end

    def test_create_time_tz_with_invalid_string_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_time_tz('not a time') }
    end

    def test_create_time_tz_with_nil_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_time_tz(nil) }
    end

    def test_create_timestamp_with_time
      time = Time.local(2026, 7, 12, 12, 34, 56, 789_012)
      value = DuckDB::Value.create_timestamp(time)

      assert_instance_of(DuckDB::Value, value)
      assert_equal(time, value.to_ruby)
    end

    def test_create_timestamp_with_string
      expected = Time.local(2026, 7, 12, 12, 34, 56, 789_000)

      assert_equal(expected, DuckDB::Value.create_timestamp('2026-07-12 12:34:56.789').to_ruby)
    end

    def test_create_timestamp_with_date
      assert_equal(Time.local(2026, 7, 12), DuckDB::Value.create_timestamp(Date.new(2026, 7, 12)).to_ruby)
    end

    def test_create_timestamp_with_invalid_string_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_timestamp('not a timestamp') }
    end

    def test_create_timestamp_with_nil_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_timestamp(nil) }
    end

    def test_create_timestamp_s_with_time_truncates_to_seconds
      time = Time.local(2026, 7, 12, 12, 34, 56, 789_012)
      value = DuckDB::Value.create_timestamp_s(time)

      assert_instance_of(DuckDB::Value, value)
      assert_equal(Time.local(2026, 7, 12, 12, 34, 56), value.to_ruby)
    end

    def test_create_timestamp_s_with_string
      expected = Time.local(2026, 7, 12, 12, 34, 56)

      assert_equal(expected, DuckDB::Value.create_timestamp_s('2026-07-12 12:34:56').to_ruby)
    end

    def test_create_timestamp_s_with_invalid_string_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_timestamp_s('not a timestamp') }
    end

    def test_create_timestamp_s_with_nil_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_timestamp_s(nil) }
    end

    def test_create_timestamp_ms_with_time_preserves_milliseconds
      time = Time.local(2026, 7, 12, 12, 34, 56, 789_012)
      value = DuckDB::Value.create_timestamp_ms(time)

      assert_instance_of(DuckDB::Value, value)
      assert_equal(Time.local(2026, 7, 12, 12, 34, 56, 789_000), value.to_ruby)
    end

    def test_create_timestamp_ms_with_string
      expected = Time.local(2026, 7, 12, 12, 34, 56, 789_000)

      assert_equal(expected, DuckDB::Value.create_timestamp_ms('2026-07-12 12:34:56.789').to_ruby)
    end

    def test_create_timestamp_ms_with_invalid_string_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_timestamp_ms('not a timestamp') }
    end

    def test_create_timestamp_ms_with_nil_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_timestamp_ms(nil) }
    end

    def test_create_timestamp_ns_with_time_preserves_nanoseconds
      time = Time.local(2026, 7, 12, 12, 34, 56, Rational(123_456_789, 1000))
      value = DuckDB::Value.create_timestamp_ns(time)

      assert_instance_of(DuckDB::Value, value)
      result = value.to_ruby

      assert_equal(time, result)
      assert_equal(123_456_789, result.nsec)
    end

    def test_create_timestamp_ns_with_string
      result = DuckDB::Value.create_timestamp_ns('2026-07-12 12:34:56.123456789').to_ruby

      assert_equal(Time.local(2026, 7, 12, 12, 34, 56, Rational(123_456_789, 1000)), result)
      assert_equal(123_456_789, result.nsec)
    end

    def test_create_timestamp_ns_with_invalid_string_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_timestamp_ns('not a timestamp') }
    end

    def test_create_timestamp_ns_with_nil_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_timestamp_ns(nil) }
    end

    def test_create_timestamp_tz_with_time_stores_instant
      time = Time.new(2026, 7, 12, 12, 34, 56, '+05:30')
      value = DuckDB::Value.create_timestamp_tz(time)

      assert_instance_of(DuckDB::Value, value)
      assert_equal(time, value.to_ruby)
    end

    def test_create_timestamp_tz_with_string
      expected = Time.parse('2026-07-12 12:34:56.789 +0000')

      assert_equal(expected, DuckDB::Value.create_timestamp_tz('2026-07-12 12:34:56.789 +0000').to_ruby)
    end

    def test_create_timestamp_tz_with_invalid_string_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_timestamp_tz('not a timestamp') }
    end

    def test_create_timestamp_tz_with_nil_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_timestamp_tz(nil) }
    end

    def test_create_date_with_invalid_string_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_date('not a date') }
    end

    def test_create_date_with_nil_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_date(nil) }
    end
  end
end
