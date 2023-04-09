
require 'test_helper'

module DuckDBTest
  class ResultToDecimalTest < Minitest::Test
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

    def do_result_to_decimal_test(value)
      prepare_test_value(value)
      result = @con.query('SELECT decimal_value FROM decimals')
      assert_equal(value, result.first.first)
      assert_instance_of(BigDecimal, result.first.first)
    end

    def test_result_to_decimal_positive1
      do_result_to_decimal_test(1.23456789)
    end
  end
end
