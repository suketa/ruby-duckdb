require 'test_helper'

module DuckDBTest
  class ResultHugeintInternalTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      @con.query('CREATE TABLE hugeints (hugeint_value HUGEINT)')
    end

    def prepare_test_value(value)
      @con.query("INSERT INTO hugeints VALUES (#{value})")
    end

    def teardown
      @con.close
      @db.close
    end

    def do_result_hugeint_internal_test(value, lower, upper)
      prepare_test_value(value)
      result = @con.query('SELECT hugeint_value FROM hugeints')
      assert_equal([lower, upper], result.send(:_to_hugeint_internal, 0, 0))
    end

    def test_result_internal_positive1
      do_result_hugeint_internal_test(1, 1, 0)
    end

    def test_hugeint_internal_zero
      do_result_hugeint_internal_test(0, 0, 0)
    end

    def test_hugeint_internal_negative1
      do_result_hugeint_internal_test(-1, -1, -1)
    end

    def test_hugeint_internal_positive100
      do_result_hugeint_internal_test(100, 100, 0)
    end

    def test_hugeint_internal_val_negative100
      do_result_hugeint_internal_test(-100, -100, -1)
    end

    def test_hugeint_internal_negative_half_hugeint
      do_result_hugeint_internal_test(-9_223_372_036_854_775_808, -9_223_372_036_854_775_808, -1)
    end

    def test_hugeint_internal_negative_half_hugeint_minus_one
      do_result_hugeint_internal_test(-9_223_372_036_854_775_809, 9_223_372_036_854_775_807, -1)
    end

    def test_hugeint_internal_val_max
      do_result_hugeint_internal_test(
        170_141_183_460_469_231_731_687_303_715_884_105_727,
        -1, 9_223_372_036_854_775_807
      )
    end

    def test_hugeint_internal_val_min
      do_result_hugeint_internal_test(
        -170_141_183_460_469_231_731_687_303_715_884_105_727,
        1, -9_223_372_036_854_775_808
      )
    end
  end
end
