# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ValueListArrayTest < Minitest::Test
    def int_values(*nums)
      nums.map { |n| DuckDB::Value.create_int32(n) }
    end

    def integer_type
      DuckDB::LogicalType.resolve(:integer)
    end

    def test_create_list
      list = DuckDB::Value.create_list(integer_type, int_values(1, 2, 3))

      assert_instance_of(DuckDB::Value, list)
      assert_equal(3, list.list_size)
    end

    def test_list_child
      list = DuckDB::Value.create_list(integer_type, int_values(10, 20))
      child = list.list_child(1)

      assert_instance_of(DuckDB::Value, child)
    end

    def test_list_child_out_of_range
      list = DuckDB::Value.create_list(integer_type, int_values(10, 20))

      assert_raises(IndexError) { list.list_child(2) }
    end

    def test_list_to_ruby
      list = DuckDB::Value.create_list(integer_type, int_values(1, 2, 3))

      assert_equal([1, 2, 3], list.to_ruby)
    end

    def test_scalar_to_ruby
      assert_equal(42, DuckDB::Value.create_int32(42).to_ruby)
    end

    def test_empty_list_to_ruby
      list = DuckDB::Value.create_list(integer_type, [])

      assert_empty(list.to_ruby)
    end

    def test_list_with_null_element_to_ruby
      values = [DuckDB::Value.create_int32(1), DuckDB::Value.create_null]
      list = DuckDB::Value.create_list(integer_type, values)

      assert_equal([1, nil], list.to_ruby)
    end

    def test_create_array
      array = DuckDB::Value.create_array(integer_type, int_values(10, 20, 30))

      assert_instance_of(DuckDB::Value, array)
      assert_equal([10, 20, 30], array.to_ruby)
    end

    def test_array_with_null_element_to_ruby
      values = [DuckDB::Value.create_int32(1), DuckDB::Value.create_null]
      array = DuckDB::Value.create_array(integer_type, values)

      assert_equal([1, nil], array.to_ruby)
    end

    def test_array_of_list_to_ruby
      list_type = DuckDB::LogicalType.create_list(:integer)
      inner1 = DuckDB::Value.create_list(integer_type, int_values(1))
      inner2 = DuckDB::Value.create_list(integer_type, int_values(2, 3))
      array = DuckDB::Value.create_array(list_type, [inner1, inner2])

      assert_equal([[1], [2, 3]], array.to_ruby)
    end

    def test_create_list_with_invalid_element
      assert_raises(ArgumentError) { DuckDB::Value.create_list(integer_type, [1, 2]) }
    end

    def test_create_list_with_invalid_type
      assert_raises(ArgumentError) { DuckDB::Value.create_list(:integer, int_values(1)) }
    end

    def test_create_array_with_invalid_element
      assert_raises(ArgumentError) { DuckDB::Value.create_array(integer_type, [1]) }
    end

    def test_bind_list_value_to_prepared_statement
      db = DuckDB::Database.open
      con = db.connect
      list = DuckDB::Value.create_list(integer_type, int_values(1, 2, 3))
      stmt = con.prepared_statement('SELECT list_sum(?::INTEGER[]) AS s')
      stmt.bind_value(1, list)

      assert_equal(6, stmt.execute.first.first)
    ensure
      con&.close
      db&.close
    end

    def test_nested_list_to_ruby
      list_type = DuckDB::LogicalType.create_list(:integer)
      inner1 = DuckDB::Value.create_list(integer_type, int_values(1))
      inner2 = DuckDB::Value.create_list(integer_type, int_values(2, 3))
      list = DuckDB::Value.create_list(list_type, [inner1, inner2])

      assert_equal([[1], [2, 3]], list.to_ruby)
    end
  end
end
