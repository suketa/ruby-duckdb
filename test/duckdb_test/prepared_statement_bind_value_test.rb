# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class PreparedStatementBindValueTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
    end

    def teardown
      @con&.close
      @db&.close
    end

    def test_bind_value_with_string_raises_argument_error
      @con.query('CREATE TABLE test_bind_value_str (col_boolean BOOLEAN)')
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO test_bind_value_str VALUES (?)')
      assert_raises(ArgumentError) do
        stmt.bind_value(1, 'string')
      end
    end

    def test_bind_value_with_true_raises_argument_error
      @con.query('CREATE TABLE test_bind_value_true (col_boolean BOOLEAN)')
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO test_bind_value_true VALUES (?)')
      assert_raises(ArgumentError) do
        stmt.bind_value(1, true)
      end
    end

    def test_bind_value_bool_true_end_to_end
      @con.query('CREATE TABLE e2e_bool_true (id INTEGER, flag BOOLEAN)')
      insert_stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO e2e_bool_true VALUES (1, ?)')
      value = DuckDB::Value.create_bool(true)

      assert_same insert_stmt, insert_stmt.bind_value(1, value)
      insert_stmt.execute

      result = @con.query('SELECT flag FROM e2e_bool_true WHERE id = 1')

      assert(result.first[0])
    end

    def test_bind_value_bool_false_end_to_end
      @con.query('CREATE TABLE e2e_bool_false (id INTEGER, flag BOOLEAN)')
      insert_stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO e2e_bool_false VALUES (1, ?)')
      value = DuckDB::Value.create_bool(false)
      insert_stmt.bind_value(1, value)
      insert_stmt.execute

      result = @con.query('SELECT flag FROM e2e_bool_false WHERE id = 1')

      refute(result.first[0])
    end

    def test_bind_value_bool_where_clause_filtering_true
      create_and_insert_bool_filter_data

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT id, active FROM e2e_bool_filter WHERE active = ?')
      stmt.bind_value(1, DuckDB::Value.create_bool(true))
      rows = stmt.execute.to_a

      assert_equal 1, rows.size
      assert_equal 1, rows[0][0]

      assert(rows[0][1])
    end

    def test_bind_value_bool_where_clause_filtering_false
      create_and_insert_bool_filter_data

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT id, active FROM e2e_bool_filter WHERE active = ?')
      stmt.bind_value(1, DuckDB::Value.create_bool(false))
      rows = stmt.execute.to_a

      assert_equal 1, rows.size
      assert_equal 2, rows[0][0]

      refute(rows[0][1])
    end

    private

    def create_and_insert_bool_filter_data
      @con.query('CREATE TABLE e2e_bool_filter (id INTEGER, active BOOLEAN)')

      insert_true = DuckDB::PreparedStatement.new(@con, 'INSERT INTO e2e_bool_filter VALUES (1, ?)')
      insert_true.bind_value(1, DuckDB::Value.create_bool(true))
      insert_true.execute

      insert_false = DuckDB::PreparedStatement.new(@con, 'INSERT INTO e2e_bool_filter VALUES (2, ?)')
      insert_false.bind_value(1, DuckDB::Value.create_bool(false))
      insert_false.execute
    end
  end
end
