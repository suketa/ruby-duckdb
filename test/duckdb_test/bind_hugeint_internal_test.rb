require 'test_helper'

module DuckDBTest
  class DuckDBBindHugeintInternalTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      @con.query('CREATE TABLE hugeints (hugeint_value HUGEINT)')
    end

    def prepare_test_value(value)
      @con.query("INSERT INTO hugeints VALUES (#{value})")
    end

    def do_bind_internal_test(value)
      prepare_test_value(value)
      stmt = @con.prepared_statement('SELECT hugeint_value FROM hugeints WHERE hugeint_value = ?')
      stmt.bind_hugeint_internal(1, value)
      result = stmt.execute
      assert_equal(value, result.first[0])
    end

    def test_bind_internal_positive1
      do_bind_internal_test(1)
    end

    def test_bind_internal_zero
      do_bind_internal_test(0)
    end

    def test_bind_internal_negative1
      do_bind_internal_test(-1)
    end

    def test_bind_internal_positive100
      do_bind_internal_test(100)
    end

    def test_bind_internal_val_negative100
      do_bind_internal_test(-100)
    end

    def test_bind_internal_val_max
      do_bind_internal_test(170_141_183_460_469_231_731_687_303_715_884_105_727)
    end

    def test_bind_internal_val_min
      do_bind_internal_test(-170_141_183_460_469_231_731_687_303_715_884_105_727)
    end

    def test_bind_internal_raises_error
      exception = assert_raises(ArgumentError) do
        do_bind_internal_test('170141183460469231731687303715884105727')
      end
      assert_equal('The argument `170141183460469231731687303715884105727` must be Integer.', exception.message)
    end

    def teardown
      @con.close
      @db.close
    end
  end
end
