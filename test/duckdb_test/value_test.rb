# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ValueTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
    end

    def teardown
      @con&.close
      @db&.close
    end

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
      value = DuckDB::Value.create_int16(32_767)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_int16_with_min
      value = DuckDB::Value.create_int16(-32_768)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_int16_with_string_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_int16('invalid')
      end
    end

    def test_create_int16_with_overflow_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_int16(32_768)
      end
    end

    def test_create_int16_with_underflow_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_int16(-32_769)
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
      @con.query('CREATE TABLE e2e_int16 (id INTEGER, val SMALLINT)')
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO e2e_int16 VALUES (1, ?)')
      stmt.bind_value(1, DuckDB::Value.create_int16(32_767))
      stmt.execute
      result = @con.query('SELECT val FROM e2e_int16 WHERE id = 1')

      assert_equal(32_767, result.first[0])
    end

    def test_create_int32_with_zero
      value = DuckDB::Value.create_int32(0)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_int32_with_max
      value = DuckDB::Value.create_int32(2_147_483_647)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_int32_with_min
      value = DuckDB::Value.create_int32(-2_147_483_648)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_int32_with_string_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_int32('invalid')
      end
    end

    def test_create_int32_with_overflow_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_int32(2_147_483_648)
      end
    end

    def test_create_int32_with_underflow_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_int32(-2_147_483_649)
      end
    end

    def test_create_int64_with_zero
      value = DuckDB::Value.create_int64(0)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_int64_with_max
      value = DuckDB::Value.create_int64(9_223_372_036_854_775_807)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_int64_with_min
      value = DuckDB::Value.create_int64(-9_223_372_036_854_775_808)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_int64_with_string_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_int64('invalid')
      end
    end

    def test_create_int64_with_overflow_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_int64(9_223_372_036_854_775_808)
      end
    end

    def test_create_int64_with_underflow_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_int64(-9_223_372_036_854_775_809)
      end
    end

    def test_create_int32_bind_value
      @con.query('CREATE TABLE e2e_int32 (id INTEGER, val INTEGER)')
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO e2e_int32 VALUES (1, ?)')
      stmt.bind_value(1, DuckDB::Value.create_int32(2_147_483_647))
      stmt.execute
      result = @con.query('SELECT val FROM e2e_int32 WHERE id = 1')

      assert_equal(2_147_483_647, result.first[0])
    end

    def test_create_int64_bind_value
      @con.query('CREATE TABLE e2e_int64 (id INTEGER, val BIGINT)')
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO e2e_int64 VALUES (1, ?)')
      stmt.bind_value(1, DuckDB::Value.create_int64(9_223_372_036_854_775_807))
      stmt.execute
      result = @con.query('SELECT val FROM e2e_int64 WHERE id = 1')

      assert_equal(9_223_372_036_854_775_807, result.first[0])
    end
  end
end
