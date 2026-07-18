# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ValueUnionTest < Minitest::Test
    def union_type
      DuckDB::LogicalType.create_union(num: :integer, str: :varchar)
    end

    def test_create_union_with_logical_type
      value = DuckDB::Value.create_union(union_type, :num, DuckDB::Value.create_int32(42))

      assert_instance_of(DuckDB::Value, value)
      assert_equal(42, value.to_ruby)
    end

    def test_create_union_with_member_spec_hash
      value = DuckDB::Value.create_union({ num: :integer, str: :varchar }, :str, DuckDB::Value.create_varchar('x'))

      assert_equal('x', value.to_ruby)
    end

    def test_create_union_with_string_tag
      value = DuckDB::Value.create_union(union_type, 'str', DuckDB::Value.create_varchar('hello'))

      assert_equal('hello', value.to_ruby)
    end

    def test_create_union_with_unknown_tag_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_union(union_type, :missing, DuckDB::Value.create_int32(1))
      end
    end

    def test_create_union_with_mismatched_member_type_raises_error
      assert_raises(DuckDB::Error) do
        DuckDB::Value.create_union(union_type, :num, DuckDB::Value.create_varchar('not an int'))
      end
    end

    def test_create_union_with_non_union_logical_type_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_union(DuckDB::LogicalType.resolve(:integer), :num, DuckDB::Value.create_int32(1))
      end
    end

    def test_create_union_with_non_value_member_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_union(union_type, :num, 42) }
    end
  end
end
