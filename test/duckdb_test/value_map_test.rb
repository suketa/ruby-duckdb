# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ValueMapTest < Minitest::Test
    def map_type
      DuckDB::LogicalType.create_map(:varchar, :integer)
    end

    def varchar_int_entries(hash)
      hash.to_h do |k, v|
        [DuckDB::Value.create_varchar(k), DuckDB::Value.create_int32(v)]
      end
    end

    def test_create_map
      map = DuckDB::Value.create_map(map_type, varchar_int_entries('a' => 1, 'b' => 2))

      assert_instance_of(DuckDB::Value, map)
    end

    def test_map_to_ruby
      map = DuckDB::Value.create_map(map_type, varchar_int_entries('a' => 1, 'b' => 2))

      assert_equal({ 'a' => 1, 'b' => 2 }, map.to_ruby)
    end

    def test_create_map_with_hash_type
      map = DuckDB::Value.create_map({ varchar: :integer }, varchar_int_entries('a' => 1))

      assert_equal({ 'a' => 1 }, map.to_ruby)
    end

    def test_create_map_with_multi_pair_hash_type
      entries = varchar_int_entries('a' => 1)

      assert_raises(ArgumentError) { DuckDB::Value.create_map({ varchar: :integer, integer: :varchar }, entries) }
    end

    def test_integer_keyed_map_to_ruby
      entries = { DuckDB::Value.create_int32(1) => DuckDB::Value.create_varchar('x') }
      map = DuckDB::Value.create_map({ integer: :varchar }, entries)

      assert_equal({ 1 => 'x' }, map.to_ruby)
    end

    def test_empty_map_to_ruby
      map = DuckDB::Value.create_map(map_type, {})

      assert_equal({}, map.to_ruby)
    end

    def test_map_with_null_value_to_ruby
      entries = { DuckDB::Value.create_varchar('a') => DuckDB::Value.create_null }
      map = DuckDB::Value.create_map(map_type, entries)

      assert_equal({ 'a' => nil }, map.to_ruby)
    end

    def test_nested_map_with_list_to_ruby
      list_type = DuckDB::LogicalType.create_list(:integer)
      inner_values = [1, 2].map { |n| DuckDB::Value.create_int32(n) }
      entries = { DuckDB::Value.create_varchar('xs') => DuckDB::Value.create_list(:integer, inner_values) }
      map = DuckDB::Value.create_map({ varchar: list_type }, entries)

      assert_equal({ 'xs' => [1, 2] }, map.to_ruby)
    end

    def test_create_map_with_duplicate_keys
      # two distinct DuckDB::Value objects holding the same key are distinct
      # Hash keys in Ruby, so the duplicate is caught by DuckDB at create time
      entries = {
        DuckDB::Value.create_varchar('a') => DuckDB::Value.create_int32(1),
        DuckDB::Value.create_varchar('a') => DuckDB::Value.create_int32(2)
      }

      assert_raises(DuckDB::Error) { DuckDB::Value.create_map(map_type, entries) }
    end

    def test_map_size
      map = DuckDB::Value.create_map(map_type, varchar_int_entries('a' => 1, 'b' => 2))

      assert_equal(2, map.map_size)
    end

    def test_map_key
      map = DuckDB::Value.create_map(map_type, varchar_int_entries('a' => 1, 'b' => 2))

      assert_instance_of(DuckDB::Value, map.map_key(0))
    end

    def test_map_value
      map = DuckDB::Value.create_map(map_type, varchar_int_entries('a' => 1, 'b' => 2))

      assert_instance_of(DuckDB::Value, map.map_value(0))
    end

    def test_map_key_and_value_read_back
      map = DuckDB::Value.create_map(map_type, varchar_int_entries('a' => 1, 'b' => 2))

      assert_equal('b', map.map_key(1).to_ruby)
      assert_equal(2, map.map_value(1).to_ruby)
    end

    def test_map_size_on_non_map_value
      assert_equal(0, DuckDB::Value.create_int32(42).map_size)
    end

    def test_map_key_on_non_map_value
      assert_raises(IndexError) { DuckDB::Value.create_int32(42).map_key(0) }
    end

    def test_map_value_on_non_map_value
      assert_raises(IndexError) { DuckDB::Value.create_int32(42).map_value(0) }
    end

    def test_map_key_out_of_range
      map = DuckDB::Value.create_map(map_type, varchar_int_entries('a' => 1, 'b' => 2))

      assert_raises(IndexError) { map.map_key(2) }
    end

    def test_map_value_out_of_range
      map = DuckDB::Value.create_map(map_type, varchar_int_entries('a' => 1, 'b' => 2))

      assert_raises(IndexError) { map.map_value(2) }
    end

    def test_create_map_with_invalid_type
      entries = varchar_int_entries('a' => 1)

      assert_raises(ArgumentError) { DuckDB::Value.create_map(DuckDB::LogicalType.resolve(:integer), entries) }
    end

    def test_create_map_with_non_hash_entries
      assert_raises(ArgumentError) { DuckDB::Value.create_map(map_type, [DuckDB::Value.create_varchar('a')]) }
    end

    def test_create_map_with_invalid_key
      entries = { 'a' => DuckDB::Value.create_int32(1) }

      assert_raises(ArgumentError) { DuckDB::Value.create_map(map_type, entries) }
    end

    def test_create_map_with_invalid_value
      entries = { DuckDB::Value.create_varchar('a') => 1 }

      assert_raises(ArgumentError) { DuckDB::Value.create_map(map_type, entries) }
    end

    def test_create_map_casts_mismatched_key_type
      # unlike LIST/STRUCT (which fail), duckdb_create_map_value casts
      # entries to the map's key/value types
      entries = { DuckDB::Value.create_int32(9) => DuckDB::Value.create_int32(1) }
      map = DuckDB::Value.create_map(map_type, entries)

      assert_equal({ '9' => 1 }, map.to_ruby)
    end

    def test_bind_map_value_to_prepared_statement
      db = DuckDB::Database.open
      con = db.connect
      map = DuckDB::Value.create_map(map_type, varchar_int_entries('a' => 1, 'b' => 2))
      stmt = con.prepared_statement("SELECT (?)['b'] AS v")
      stmt.bind_value(1, map)

      assert_equal(2, stmt.execute.first.first)
    ensure
      con&.close
      db&.close
    end
  end
end
