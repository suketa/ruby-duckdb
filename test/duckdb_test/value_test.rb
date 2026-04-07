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

    def test_create_int8_with_zero
      value = DuckDB::Value.create_int8(0)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_int8_with_max
      value = DuckDB::Value.create_int8(127)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_int8_with_min
      value = DuckDB::Value.create_int8(-128)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_int16_with_zero
      value = DuckDB::Value.create_int16(0)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_int16_with_max
      value = DuckDB::Value.create_int16(32767)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_int16_with_min
      value = DuckDB::Value.create_int16(-32768)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_int16_with_string_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_int16('invalid')
      end
    end

    def test_create_int16_with_overflow_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_int16(32768)
      end
    end

    def test_create_int16_with_underflow_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_int16(-32769)
      end
    end

    def test_create_int8_with_string_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_int8('invalid')
      end
    end

    def test_create_int8_with_overflow_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_int8(128)
      end
    end

    def test_create_int8_with_underflow_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_int8(-129)
      end
    end

    def test_create_int16_bind_value
      db = DuckDB::Database.open
      con = db.connect
      con.query('CREATE TABLE e2e_int16 (id INTEGER, val SMALLINT)')
      insert_stmt = DuckDB::PreparedStatement.new(con, 'INSERT INTO e2e_int16 VALUES (1, ?)')
      value = DuckDB::Value.create_int16(32767)
      insert_stmt.bind_value(1, value)
      insert_stmt.execute

      result = con.query('SELECT val FROM e2e_int16 WHERE id = 1')

      assert_equal(32767, result.first[0])
    ensure
      con&.close
      db&.close
    end
  end
end
