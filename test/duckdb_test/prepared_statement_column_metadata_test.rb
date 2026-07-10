# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class PreparedStatementColumnMetadataTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      @con.query('CREATE TABLE users (id INTEGER, name VARCHAR, salary DECIMAL(9, 4))')
    end

    def teardown
      @con&.close
      @db&.close
    end

    def test_column_count
      stmt = @con.prepared_statement('SELECT id, name FROM users WHERE id = ?')

      assert_equal 2, stmt.column_count
    end

    def test_column_name
      stmt = @con.prepared_statement('SELECT id, name AS user_name FROM users WHERE id = ?')

      assert_equal 'id', stmt.column_name(0)
      assert_equal 'user_name', stmt.column_name(1)
    end

    def test_column_name_out_of_range
      stmt = @con.prepared_statement('SELECT id FROM users')
      assert_raises(DuckDB::Error) { stmt.column_name(1) }
    end

    def test_column_type
      stmt = @con.prepared_statement('SELECT id, name, salary FROM users WHERE id = ?')

      assert_equal :integer, stmt.column_type(0)
      assert_equal :varchar, stmt.column_type(1)
      assert_equal :decimal, stmt.column_type(2)
    end

    def test_column_type_out_of_range
      stmt = @con.prepared_statement('SELECT id FROM users')

      assert_equal :invalid, stmt.column_type(1)
    end

    def test_column_logical_type
      stmt = @con.prepared_statement('SELECT salary FROM users WHERE id = ?')
      logical_type = stmt.column_logical_type(0)

      assert_equal :decimal, logical_type.type
      assert_equal 9, logical_type.width
      assert_equal 4, logical_type.scale
    end

    def test_column_logical_type_out_of_range
      stmt = @con.prepared_statement('SELECT id FROM users')

      assert_raises(DuckDB::Error) { stmt.column_logical_type(1) }
    end

    # When any result column type is ambiguous (untyped parameter),
    # metadata collapses to a single column of invalid type named 'unknown'.
    def test_ambiguous_column_types
      stmt = @con.prepared_statement('SELECT $1::TEXT, $2::INTEGER, $3')

      assert_equal 1, stmt.column_count
      assert_equal :invalid, stmt.column_type(0)
      assert_equal :invalid, stmt.column_logical_type(0).type
    end

    def test_ambiguous_column_name
      stmt = @con.prepared_statement('SELECT $1::TEXT, $2::INTEGER, $3')

      assert_equal 'unknown', stmt.column_name(0)
      assert_raises(DuckDB::Error) { stmt.column_name(1) }
    end
  end
end
