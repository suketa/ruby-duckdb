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

    def test_create_uint8_with_zero
      value = DuckDB::Value.create_uint8(0)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_uint8_with_max
      value = DuckDB::Value.create_uint8(255)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_uint8_with_positive
      value = DuckDB::Value.create_uint8(128)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_uint8_with_string_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_uint8('invalid')
      end
    end

    def test_create_uint8_with_overflow_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_uint8(256)
      end
    end

    def test_create_uint8_with_negative_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_uint8(-1)
      end
    end

    def test_create_uint16_with_zero
      value = DuckDB::Value.create_uint16(0)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_uint16_with_max
      value = DuckDB::Value.create_uint16(65_535)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_uint16_with_positive
      value = DuckDB::Value.create_uint16(32_768)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_uint16_with_string_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_uint16('invalid')
      end
    end

    def test_create_uint16_with_overflow_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_uint16(65_536)
      end
    end

    def test_create_uint16_with_negative_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_uint16(-1)
      end
    end

    def test_create_uint32_with_zero
      value = DuckDB::Value.create_uint32(0)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_uint32_with_max
      value = DuckDB::Value.create_uint32(4_294_967_295)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_uint32_with_positive
      value = DuckDB::Value.create_uint32(2_147_483_648)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_uint32_with_string_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_uint32('invalid')
      end
    end

    def test_create_uint32_with_overflow_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_uint32(4_294_967_296)
      end
    end

    def test_create_uint32_with_negative_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_uint32(-1)
      end
    end

    def test_create_uint64_with_zero
      value = DuckDB::Value.create_uint64(0)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_uint64_with_max
      value = DuckDB::Value.create_uint64(18_446_744_073_709_551_615)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_uint64_with_positive
      value = DuckDB::Value.create_uint64(9_223_372_036_854_775_808)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_uint64_with_string_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_uint64('invalid')
      end
    end

    def test_create_uint64_with_overflow_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_uint64(18_446_744_073_709_551_616)
      end
    end

    def test_create_uint64_with_negative_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_uint64(-1)
      end
    end

    def test_create_null
      value = DuckDB::Value.create_null

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_float_with_zero
      value = DuckDB::Value.create_float(0.0)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_float_with_positive
      value = DuckDB::Value.create_float(1.5)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_float_with_negative
      value = DuckDB::Value.create_float(-1.5)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_float_with_integer
      value = DuckDB::Value.create_float(42)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_float_with_string_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_float('invalid')
      end
    end

    def test_create_double_with_zero
      value = DuckDB::Value.create_double(0.0)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_double_with_positive
      value = DuckDB::Value.create_double(1.5)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_double_with_negative
      value = DuckDB::Value.create_double(-1.5)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_double_with_integer
      value = DuckDB::Value.create_double(42)

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_double_with_string_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_double('invalid')
      end
    end

    def test_create_varchar
      value = DuckDB::Value.create_varchar('Hello')

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_varchar_with_empty_string
      value = DuckDB::Value.create_varchar('')

      assert_instance_of(DuckDB::Value, value)
    end

    def test_create_varchar_with_integer_raises_argument_error
      assert_raises(ArgumentError) do
        DuckDB::Value.create_varchar(123)
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

    def test_create_uint8_bind_value
      @con.query('CREATE TABLE e2e_uint8 (id INTEGER, val UTINYINT)')
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO e2e_uint8 VALUES (1, ?)')
      stmt.bind_value(1, DuckDB::Value.create_uint8(255))
      stmt.execute
      result = @con.query('SELECT val FROM e2e_uint8 WHERE id = 1')

      assert_equal(255, result.first[0])
    end

    def test_create_uint16_bind_value
      @con.query('CREATE TABLE e2e_uint16 (id INTEGER, val USMALLINT)')
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO e2e_uint16 VALUES (1, ?)')
      stmt.bind_value(1, DuckDB::Value.create_uint16(65_535))
      stmt.execute
      result = @con.query('SELECT val FROM e2e_uint16 WHERE id = 1')

      assert_equal(65_535, result.first[0])
    end

    def test_create_uint32_bind_value
      @con.query('CREATE TABLE e2e_uint32 (id INTEGER, val UINTEGER)')
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO e2e_uint32 VALUES (1, ?)')
      stmt.bind_value(1, DuckDB::Value.create_uint32(4_294_967_295))
      stmt.execute
      result = @con.query('SELECT val FROM e2e_uint32 WHERE id = 1')

      assert_equal(4_294_967_295, result.first[0])
    end

    def test_create_uint64_bind_value
      @con.query('CREATE TABLE e2e_uint64 (id INTEGER, val UBIGINT)')
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO e2e_uint64 VALUES (1, ?)')
      stmt.bind_value(1, DuckDB::Value.create_uint64(18_446_744_073_709_551_615))
      stmt.execute
      result = @con.query('SELECT val FROM e2e_uint64 WHERE id = 1')

      assert_equal(18_446_744_073_709_551_615, result.first[0])
    end

    def test_create_null_bind_value
      @con.query('CREATE TABLE e2e_null (id INTEGER, val INTEGER)')
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO e2e_null VALUES (1, ?)')
      stmt.bind_value(1, DuckDB::Value.create_null)
      stmt.execute
      result = @con.query('SELECT val FROM e2e_null WHERE id = 1')

      assert_nil(result.first[0])
    end

    def test_create_float_bind_value
      @con.query('CREATE TABLE e2e_float (id INTEGER, val FLOAT)')
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO e2e_float VALUES (1, ?)')
      stmt.bind_value(1, DuckDB::Value.create_float(1.5))
      stmt.execute
      result = @con.query('SELECT val FROM e2e_float WHERE id = 1')

      assert_in_delta(1.5, result.first[0], 0.001)
    end

    def test_create_double_bind_value
      @con.query('CREATE TABLE e2e_double (id INTEGER, val DOUBLE)')
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO e2e_double VALUES (1, ?)')
      stmt.bind_value(1, DuckDB::Value.create_double(1.7976931348623157))
      stmt.execute
      result = @con.query('SELECT val FROM e2e_double WHERE id = 1')

      assert_in_delta(1.7976931348623157, result.first[0], 1e-15)
    end

    def test_create_varchar_bind_value
      @con.query('CREATE TABLE e2e_varchar (id INTEGER, val VARCHAR)')
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO e2e_varchar VALUES (1, ?)')
      stmt.bind_value(1, DuckDB::Value.create_varchar('Hello DuckDB'))
      stmt.execute
      result = @con.query('SELECT val FROM e2e_varchar WHERE id = 1')

      assert_equal('Hello DuckDB', result.first[0])
    end
  end
end
