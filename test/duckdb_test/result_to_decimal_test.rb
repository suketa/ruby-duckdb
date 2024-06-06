# frozen_string_literal: true

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

      # fix for using duckdb_fetch_chunk in Result#chunk_each
      result = result.to_a

      assert_equal(value, result.first.first)
      assert_instance_of(BigDecimal, result.first.first)
    end

    def test_result_to_decimal_positive1
      do_result_to_decimal_test(1.23456789)
    end

    def test_result_to_decimal_positive2
      do_result_to_decimal_test(123.456789)
    end

    def test_result_to_decimal_positive3
      do_result_to_decimal_test(123_456_789)
    end

    def test_result_to_decimal_zero
      do_result_to_decimal_test(0)
    end

    def test_result_to_decimal_one
      do_result_to_decimal_test(1)
    end

    def test_result_to_decimal_positive4
      do_result_to_decimal_test(0.00000001)
    end

    def test_result_to_decimal_positive5
      do_result_to_decimal_test(0.00000123)
    end

    def test_result_to_decimal_positive6
      do_result_to_decimal_test(0.1)
    end
  end
end
