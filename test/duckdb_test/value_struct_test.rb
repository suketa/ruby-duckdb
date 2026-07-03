# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ValueStructTest < Minitest::Test
    def struct_type
      DuckDB::LogicalType.create_struct(a: :integer, b: :varchar)
    end

    def struct_values(int_val, str_val)
      [DuckDB::Value.create_int32(int_val), DuckDB::Value.create_varchar(str_val)]
    end

    def test_create_struct
      struct = DuckDB::Value.create_struct(struct_type, struct_values(1, 'x'))

      assert_instance_of(DuckDB::Value, struct)
    end

    def test_struct_to_ruby
      struct = DuckDB::Value.create_struct(struct_type, struct_values(1, 'x'))

      assert_equal({ a: 1, b: 'x' }, struct.to_ruby)
    end

    def test_struct_with_null_field_to_ruby
      values = [DuckDB::Value.create_null, DuckDB::Value.create_varchar('x')]
      struct = DuckDB::Value.create_struct(struct_type, values)

      assert_equal({ a: nil, b: 'x' }, struct.to_ruby)
    end

    def test_nested_struct_with_list_to_ruby
      list_type = DuckDB::LogicalType.create_list(:integer)
      nested_type = DuckDB::LogicalType.create_struct(xs: list_type)
      inner_values = [1, 2].map { |n| DuckDB::Value.create_int32(n) }
      list_value = DuckDB::Value.create_list(DuckDB::LogicalType.resolve(:integer), inner_values)
      struct = DuckDB::Value.create_struct(nested_type, [list_value])

      assert_equal({ xs: [1, 2] }, struct.to_ruby)
    end

    def test_struct_child
      struct = DuckDB::Value.create_struct(struct_type, struct_values(1, 'x'))
      child = struct.struct_child(0)

      assert_instance_of(DuckDB::Value, child)
    end

    def test_struct_child_out_of_range
      struct = DuckDB::Value.create_struct(struct_type, struct_values(1, 'x'))

      assert_raises(IndexError) { struct.struct_child(2) }
    end

    def test_create_struct_with_too_few_values
      assert_raises(ArgumentError) { DuckDB::Value.create_struct(struct_type, struct_values(1, 'x')[0, 1]) }
    end

    def test_create_struct_with_too_many_values
      values = struct_values(1, 'x') + [DuckDB::Value.create_int32(2)]

      assert_raises(ArgumentError) { DuckDB::Value.create_struct(struct_type, values) }
    end

    def test_create_struct_with_invalid_type
      assert_raises(ArgumentError) { DuckDB::Value.create_struct(DuckDB::LogicalType.resolve(:integer), struct_values(1, 'x')) }
    end

    def test_create_struct_with_invalid_element
      assert_raises(ArgumentError) { DuckDB::Value.create_struct(struct_type, [1, 'x']) }
    end
  end
end
