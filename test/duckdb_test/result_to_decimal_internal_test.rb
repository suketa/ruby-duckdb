
require 'test_helper'

module DuckDBTest
  class ResultToDecimalInternalTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      @con.query('CREATE TABLE decimals (decimal_value DECIMAL(30,8))')
    end

    def prepare_test_value(value)
      @con.query("INSERT INTO decimals VALUES (#{value})")
    end

    def teardown
      @con.close
      @db.close
    end

    def do_result_to_decimal_test(expected, value)
      prepare_test_value(value)
      result = @con.query('SELECT decimal_value FROM decimals')
      assert_equal(expected, result.send(:__to_decimal_internal, 0, 0))
    end

    def test_result_to_decimal_positive1
      do_result_to_decimal_test([123_400_000, 0, 30, 8], '1.234')
    end

    def test_result_to_decimal_positive2
      do_result_to_decimal_test([123_456_789, 0, 30, 8], '1.23456789')
    end

    def test_result_to_decimal_positive3
      do_result_to_decimal_test([123_456_789, 0, 30, 8], '1.234567898')
    end

    def test_result_to_decimal_positive4
      do_result_to_decimal_test([123_456, 0, 30, 8], '0.00123456789')
    end

    def test_result_to_decimal_positive5
      do_result_to_decimal_test([123_456, 0, 30, 8], '0.00123456')
    end

    def test_result_to_decimal_positive6
      do_result_to_decimal_test([-123_456, -1, 30, 8], '-0.00123456')
    end

    def test_result_to_decimal_positive7
      do_result_to_decimal_test([6_634_324_952_100_531_253, 12_715_950_803, 30, 8], '2345678901234567890123.45678901')
    end

    def test_result_to_decimal_positive8
      do_result_to_decimal_test([-6_634_324_952_100_531_253, -12_715_950_804, 30, 8], '-2345678901234567890123.45678901')
    end
  end
end
