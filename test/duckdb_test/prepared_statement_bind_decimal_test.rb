# frozen_string_literal: true

module DuckDBTest
  class PreparedStatementDecimalTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      @con.query('CREATE TABLE decimals (decimal_value HUGEINT)')
    end

    def prepare_test_value(value)
      @con.query("INSERT INTO decimals VALUES (#{value})")
      @prepared = @con.prepared_statement('SELECT * FROM decimals WHERE decimal_value = ?')
    end

    def teardown
      @con.close
      @db.close
    end

    # FIXME: @prepared.bind(1, BigDecimal('1.0')) should not raise DuckDB::Error.
    def test_decimal
      prepare_test_value(1.0)
      r = @prepared.bind(1, BigDecimal('1.0')).execute
      assert_equal(1.0, r.first.first)
    end
  end
end
