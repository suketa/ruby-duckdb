# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ValueEnumTest < Minitest::Test
    def enum_type
      DuckDB::LogicalType.create_enum('happy', 'sad', 'neutral')
    end

    def test_create_enum_with_logical_type_and_string
      value = DuckDB::Value.create_enum(enum_type, 'sad')

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_enum_with_member_array_spec
      value = DuckDB::Value.create_enum(%w[happy sad neutral], 'happy')

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_enum_with_symbol_member
      assert_instance_of(DuckDB::Value, DuckDB::Value.create_enum(enum_type, :neutral))
    end

    def test_create_enum_with_index_member
      assert_instance_of(DuckDB::Value, DuckDB::Value.create_enum(enum_type, 2))
    end

    def test_create_enum_with_unknown_member_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_enum(enum_type, 'angry') }
    end

    def test_create_enum_with_out_of_range_index_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_enum(enum_type, 3) }
    end

    def test_create_enum_with_negative_index_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_enum(enum_type, -1) }
    end

    def test_create_enum_with_non_enum_logical_type_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_enum(DuckDB::LogicalType.resolve(:integer), 'happy')
      end
    end

    def test_create_enum_with_invalid_member_type_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_enum(enum_type, nil) }
    end
  end
end
