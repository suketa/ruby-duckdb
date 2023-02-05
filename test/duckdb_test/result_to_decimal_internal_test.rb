
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
      p result.first
      assert_equal(expected, result.send(:__to_decimal_internal, 0, 0))
    end

    def test_result_to_decimal_positive1
      do_result_to_decimal_test([123400000, 0, 30, 8], 1.234)
    end

    def test_result_to_decimal_positive2
      do_result_to_decimal_test([123456894, 0, 30, 8], 1.234568945)
    end

    def test_result_to_decimal_positive3
      do_result_to_decimal_test([123456, 0, 30, 8], 0.00123456789)
    end

    def test_result_to_decimal_positive3
      do_result_to_decimal_test([0, 0, 30, 8], "234567890123456789012.34567891")
    end
  end
end
