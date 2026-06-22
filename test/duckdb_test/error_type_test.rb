# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ErrorTypeTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
    end

    def teardown
      @con&.close
      @db&.close
    end

    def test_constraint_violation_via_query
      @con.query('CREATE TABLE t (id INTEGER PRIMARY KEY)')
      @con.query('INSERT INTO t VALUES (1)')

      err = assert_raises(DuckDB::Error) { @con.query('INSERT INTO t VALUES (1)') }
      assert_equal :constraint, err.error_type
    end

    def test_constraint_violation_via_bind
      @con.query('CREATE TABLE t (id INTEGER PRIMARY KEY)')
      @con.query('INSERT INTO t VALUES (?)', 1)

      err = assert_raises(DuckDB::Error) { @con.query('INSERT INTO t VALUES (?)', 1) }
      assert_equal :constraint, err.error_type
    end

    def test_catalog_error_for_unknown_table
      err = assert_raises(DuckDB::Error) { @con.query('SELECT * FROM no_such_table') }
      assert_equal :catalog, err.error_type
    end

    def test_error_type_is_nil_without_result_error
      assert_nil DuckDB::Error.new('boom').error_type
    end
  end
end
