# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ValueBitTest < Minitest::Test
    def test_create_bit
      value = DuckDB::Value.create_bit('0101')

      assert_instance_of(DuckDB::Value, value)
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
