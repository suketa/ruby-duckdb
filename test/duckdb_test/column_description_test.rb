# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ColumnDescriptionTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
    end

    def teardown
      @con&.close
      @db&.close
    end

    def test_name
      @con.query("CREATE TABLE t (i INTEGER, j VARCHAR DEFAULT 'foo')")
      td = DuckDB::TableDescription.new(@con, 't')
      cds = td.column_descriptions

      assert_equal('i', cds[0].name)
      assert_equal('j', cds[1].name)
    end

    def test_logical_type
      @con.query("CREATE TABLE t (i INTEGER, j VARCHAR DEFAULT 'foo')")
      td = DuckDB::TableDescription.new(@con, 't')
      cds = td.column_descriptions

      assert_equal(:integer, cds[0].logical_type.type)
      assert_equal(:varchar, cds[1].logical_type.type)
    end

    def test_has_default
      @con.query("CREATE TABLE t (i INTEGER, j VARCHAR DEFAULT 'foo')")
      td = DuckDB::TableDescription.new(@con, 't')
      cds = td.column_descriptions

      refute_predicate(cds[0], :has_default?)
      assert_predicate(cds[1], :has_default?)
    end
  end
end
