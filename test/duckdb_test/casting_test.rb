# frozen_string_literal: true

require 'test_helper'
require 'date'

module DuckDBTest
  class CastingTest < Minitest::Test
    def test_cast_integer
      assert_equal(2_147_483_647, DuckDB.cast('2147483647', :integer))
      assert_equal(2_147_483_647, DuckDB.cast(2_147_483_647, :integer))
    end

    def test_cast_logical_type_integer
      assert_equal(2_147_483_647, DuckDB.cast('2147483647', DuckDB::LogicalType::INTEGER))
    end

    def test_cast_integer_invalid
      assert_raises(ArgumentError) { DuckDB.cast('abc', :integer) }
    end

    def test_cast_bigint
      assert_equal(9_223_372_036_854_775_807, DuckDB.cast('9223372036854775807', :bigint))
      assert_equal(9_223_372_036_854_775_807, DuckDB.cast(9_223_372_036_854_775_807, :bigint))
    end

    def test_cast_hugeint
      assert_equal(
        170_141_183_460_469_231_731_687_303_715_884_105_727,
        DuckDB.cast('170141183460469231731687303715884105727', :hugeint)
      )
      assert_equal(
        170_141_183_460_469_231_731_687_303_715_884_105_727,
        DuckDB.cast(170_141_183_460_469_231_731_687_303_715_884_105_727, :hugeint)
      )
    end

    def test_cast_float
      assert_in_delta(3.14, DuckDB.cast('3.14', :float))
      assert_in_delta(3.14, DuckDB.cast(3.14, :float))
    end

    def test_cast_double
      assert_in_delta(3.14, DuckDB.cast('3.14', :double))
      assert_in_delta(3.14, DuckDB.cast(3.14, :double))
    end

    def test_cast_varchar
      assert_equal('abc', DuckDB.cast('abc', :varchar))
      assert_equal('1', DuckDB.cast(1, :varchar))
    end

    def test_cast_timestamp
      assert_equal(Time.new(2024, 1, 2, 3, 4, 5), DuckDB.cast('2024-01-02 03:04:05', :timestamp))
      assert_equal(Time.new(2024, 1, 2, 3, 4, 5), DuckDB.cast(Time.new(2024, 1, 2, 3, 4, 5), :timestamp))
      assert_equal(Time.new(2024, 1, 2, 3, 4, 5, '+0000'), DuckDB.cast(DateTime.new(2024, 1, 2, 3, 4, 5), :timestamp))
    end

    def test_cast_timestamp_invalid
      assert_raises(ArgumentError) { DuckDB.cast('invalid date', :timestamp) }
    end

    def test_cast_date
      assert_equal(Date.new(2024, 1, 2), DuckDB.cast('2024-01-02', :date))
      assert_equal(Date.new(2024, 1, 2), DuckDB.cast(Date.new(2024, 1, 2), :date))
    end
  end
end
