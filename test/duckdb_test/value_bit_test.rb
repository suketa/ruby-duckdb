# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ValueBitTest < Minitest::Test
    def test_create_bit
      value = DuckDB::Value.create_bit('0101')

      assert_instance_of(DuckDB::Value, value)
    end

    def test_to_ruby_round_trips
      assert_equal('0101', DuckDB::Value.create_bit('0101').to_ruby)
    end

    def test_to_ruby_round_trips_with_length_not_divisible_by_eight
      bits = '010110011'

      assert_equal(bits, DuckDB::Value.create_bit(bits).to_ruby)
    end

    def test_to_ruby_round_trips_with_multiple_bytes
      bits = '1010101010101010'

      assert_equal(bits, DuckDB::Value.create_bit(bits).to_ruby)
    end

    def test_to_ruby_with_malformed_bit_raises_error
      ["\x80\x01", "\x08\x01", "\x00", ''].each do |raw|
        value = DuckDB::Value.send(:_create_bit, raw.b)
        error = assert_raises(DuckDB::Error) { value.to_ruby }

        assert_equal('invalid BIT value', error.message)
      end
    end

    def test_create_bit_with_invalid_characters_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_bit('01012') }
    end

    def test_create_bit_with_empty_string_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_bit('') }
    end

    def test_create_bit_with_nil_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_bit(nil) }
    end
  end
end
