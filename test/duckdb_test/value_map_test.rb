# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ValueMapTest < Minitest::Test
    def map_type
      DuckDB::LogicalType.create_map(:varchar, :integer)
    end

    def map_keys(*strs)
      strs.map { |s| DuckDB::Value.create_varchar(s) }
    end

    def map_values(*ints)
      ints.map { |i| DuckDB::Value.create_int32(i) }
    end

    def test_create_map
      map = DuckDB::Value.create_map(map_type, map_keys('a', 'b'), map_values(1, 2))

      assert_instance_of(DuckDB::Value, map)
    end

    def test_map_to_ruby
      map = DuckDB::Value.create_map(map_type, map_keys('a', 'b'), map_values(1, 2))

      assert_equal({ 'a' => 1, 'b' => 2 }, map.to_ruby)
    end

    def test_integer_keyed_map_to_ruby
      type = DuckDB::LogicalType.create_map(:integer, :varchar)
      keys = [DuckDB::Value.create_int32(1)]
      values = [DuckDB::Value.create_varchar('x')]
      map = DuckDB::Value.create_map(type, keys, values)

      assert_equal({ 1 => 'x' }, map.to_ruby)
    end

    def test_empty_map_to_ruby
      map = DuckDB::Value.create_map(map_type, [], [])

      assert_equal({}, map.to_ruby)
    end

    def test_map_with_null_value_to_ruby
      keys = map_keys('a')
      values = [DuckDB::Value.create_null]
      map = DuckDB::Value.create_map(map_type, keys, values)

      assert_equal({ 'a' => nil }, map.to_ruby)
    end

    def test_nested_map_with_list_to_ruby
      list_type = DuckDB::LogicalType.create_list(:integer)
      type = DuckDB::LogicalType.create_map(:varchar, list_type)
      keys = map_keys('xs')
      inner_values = [1, 2].map { |n| DuckDB::Value.create_int32(n) }
      list_value = DuckDB::Value.create_list(DuckDB::LogicalType.resolve(:integer), inner_values)
      map = DuckDB::Value.create_map(type, keys, [list_value])

      assert_equal({ 'xs' => [1, 2] }, map.to_ruby)
    end

    def test_create_map_with_duplicate_keys
      keys = map_keys('a', 'a')
      values = map_values(1, 2)

      assert_raises(DuckDB::Error) { DuckDB::Value.create_map(map_type, keys, values) }
    end

    def test_map_size
      map = DuckDB::Value.create_map(map_type, map_keys('a', 'b'), map_values(1, 2))

      assert_equal(2, map.map_size)
    end

    def test_map_key
      map = DuckDB::Value.create_map(map_type, map_keys('a', 'b'), map_values(1, 2))

      assert_instance_of(DuckDB::Value, map.map_key(0))
    end

    def test_map_value
      map = DuckDB::Value.create_map(map_type, map_keys('a', 'b'), map_values(1, 2))

      assert_instance_of(DuckDB::Value, map.map_value(0))
    end

    def test_map_key_out_of_range
      map = DuckDB::Value.create_map(map_type, map_keys('a', 'b'), map_values(1, 2))

      assert_raises(IndexError) { map.map_key(2) }
    end

    def test_map_value_out_of_range
      map = DuckDB::Value.create_map(map_type, map_keys('a', 'b'), map_values(1, 2))

      assert_raises(IndexError) { map.map_value(2) }
    end

    def test_create_map_with_mismatched_sizes
      assert_raises(ArgumentError) { DuckDB::Value.create_map(map_type, map_keys('a', 'b'), map_values(1)) }
    end

    def test_create_map_with_invalid_type
      assert_raises(ArgumentError) { DuckDB::Value.create_map(DuckDB::LogicalType.resolve(:integer), map_keys('a'), map_values(1)) }
    end

    def test_create_map_with_invalid_element
      assert_raises(ArgumentError) { DuckDB::Value.create_map(map_type, ['a'], map_values(1)) }
    end
  end
end
