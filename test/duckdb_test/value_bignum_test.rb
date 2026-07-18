# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ValueBignumTest < Minitest::Test
    BEYOND_HUGEINT = (2**200) + 123_456_789

    def test_create_bignum
      value = DuckDB::Value.create_bignum(BEYOND_HUGEINT)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_bignum_with_negative
      value = DuckDB::Value.create_bignum(-BEYOND_HUGEINT)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_bignum_with_zero
      assert_instance_of(DuckDB::Value, DuckDB::Value.create_bignum(0))
    end

    def test_to_ruby_round_trips_beyond_hugeint_range
      assert_equal(BEYOND_HUGEINT, DuckDB::Value.create_bignum(BEYOND_HUGEINT).to_ruby)
    end

    def test_to_ruby_round_trips_negative
      assert_equal(-BEYOND_HUGEINT, DuckDB::Value.create_bignum(-BEYOND_HUGEINT).to_ruby)
    end

    def test_to_ruby_round_trips_small_values
      [0, 1, -1, 255, -256].each do |i|
        assert_equal(i, DuckDB::Value.create_bignum(i).to_ruby)
      end
    end

    def test_create_bignum_with_string_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_bignum('123') }
    end

    def test_create_bignum_with_nil_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_bignum(nil) }
    end
  end
end
