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
      time = Time.local(2026, 7, 12, 12, 34, 56, Rational(123_456_789, 1000))
      value = DuckDB::Value.create_time_ns(time)

      assert_instance_of(DuckDB::Value, value)
      result = value.to_ruby

      assert_equal([12, 34, 56, 123_456_789], [result.hour, result.min, result.sec, result.nsec])
    end

    def test_create_time_ns_with_string
      result = DuckDB::Value.create_time_ns('12:34:56.123456789').to_ruby

      assert_equal(123_456_789, result.nsec)
    end

    def test_create_time_ns_with_invalid_string_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_time_ns('not a time') }
    end

    def test_create_time_ns_with_nil_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_time_ns(nil) }
    end

    def test_create_date_with_invalid_string_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_date('not a date') }
    end

    def test_create_date_with_nil_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_date(nil) }
    end
  end
end
