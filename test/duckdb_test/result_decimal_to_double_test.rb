
require 'test_helper'

module DuckDBTest
  class ResultDecimalToDoubleTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      @con.query('CREATE TABLE decimals (decimal_value DECIMAL(38, 10))')
    end

    def prepare_test_value(value)
      @con.query("INSERT INTO decimals VALUES (#{value})")
    end

    def teardown
      @con.close
      @db.close
    end

    def do_result_decimal_to_double_internal_test(value)
      prepare_test_value(value)
      result = @con.query('SELECT decimal_value FROM decimals')
      assert_equal(value, result.send(:_decimal_to_double, 0, 0))
    end

    def test_result_internal_positive1
      do_result_decimal_to_double_internal_test(1.2345678901)
    end

    def test_decimal_to_double_zero
      do_result_decimal_to_double_internal_test(0)
    end

    def test_decimal_to_double_negative1
      do_result_decimal_to_double_internal_test(-1.2345678901)
    end

    def test_decimal_to_double_positive100
      do_result_decimal_to_double_internal_test(100.1234567890)
    end

    def test_decimal_to_double_val_negative100
      do_result_decimal_to_double_internal_test(-100.1234567890)
    end

    def test_decimal_to_double_negative_half_hugeint
      do_result_decimal_to_double_internal_test(-9_223_372_036_854_775_808.1234567890)
    end

    def test_decimal_to_double_negative_half_hugeint_minus_one
      do_result_decimal_to_double_internal_test(-9_223_372_036_854_775_809.123456890)
    end

    def test_hugeint_to_double_half_hugeint
      do_result_decimal_to_double_internal_test(9_223_372_036_854_775_808.123456890)
    end

    def test_hugeint_to_double_half_hugeint_plus_one
      do_result_decimal_to_double_internal_test(9_223_372_036_854_775_809.123456890)
    end
  end
end
