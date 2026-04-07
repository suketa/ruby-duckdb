# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ValueTest < Minitest::Test
    def test_create_bool_with_true
      value = DuckDB::Value.create_bool(true)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_bool_with_false
      value = DuckDB::Value.create_bool(false)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_bool_with_string_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_bool('invalid')
      end
    end

    def test_create_bool_with_nil_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_bool(nil)
      end
    end

    def test_create_bool_with_integer_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_bool(1)
      end
    end
  end
end
