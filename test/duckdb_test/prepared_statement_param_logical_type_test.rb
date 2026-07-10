# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class PreparedStatementParamLogicalTypeTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      @con.query('CREATE TABLE users (id INTEGER, salary DECIMAL(9, 4))')
    end

    def teardown
      @con.close
      @db.close
    end

    def test_param_logical_type_integer
      stmt = @con.prepared_statement('SELECT * FROM users WHERE id = ?')
      logical_type = stmt.param_logical_type(1)

      assert_equal(:integer, logical_type.type)
    end

    def test_param_logical_type_decimal
      stmt = @con.prepared_statement('SELECT * FROM users WHERE salary = ?')
      logical_type = stmt.param_logical_type(1)

      assert_equal(:decimal, logical_type.type)
      assert_equal(9, logical_type.width)
      assert_equal(4, logical_type.scale)
    end

    def test_param_logical_type_out_of_range
      stmt = @con.prepared_statement('SELECT * FROM users WHERE id = ?')

      assert_raises(DuckDB::Error) { stmt.param_logical_type(2) }
    end
  end
end
