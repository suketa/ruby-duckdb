# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  if defined?(DuckDB::TableDescription)
  class TableDescriptionTest < Minitest::Test # rubocop:disable Layout/IndentationWidth
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      @con.query('CREATE TABLE t (i INTEGER, j INTEGER DEFAULT 5)')
    end

    def teardown
      @con.close
      @db.close
    end

    def test_s_new
      assert_instance_of DuckDB::TableDescription, DuckDB::TableDescription.new(@con, 't')
    end

    def test_s_new_with_schema
      @con.query('CREATE SCHEMA s')
      @con.query('CREATE TABLE s.u (x INTEGER)')

      assert_instance_of DuckDB::TableDescription, DuckDB::TableDescription.new(@con, 'u', schema: 's')
    end

    def test_s_new_with_catalog
      @con.query("ATTACH ':memory:' AS ext")
      @con.query('CREATE TABLE ext.v (x INTEGER)')

      assert_instance_of DuckDB::TableDescription, DuckDB::TableDescription.new(@con, 'v', catalog: 'ext')
    end

    def test_s_new_with_schema_and_catalog
      @con.query("ATTACH ':memory:' AS ext")
      @con.query('CREATE SCHEMA ext.s')
      @con.query('CREATE TABLE ext.s.w (x INTEGER)')

      assert_instance_of DuckDB::TableDescription,
                         DuckDB::TableDescription.new(@con, 'w', schema: 's', catalog: 'ext')
    end

    def test_s_new_with_invalid_connection
      assert_raises DuckDB::Error do
        DuckDB::TableDescription.new('not_a_connection', 't')
      end
    end

    def test_s_new_with_nil_table
      assert_raises DuckDB::Error do
        DuckDB::TableDescription.new(@con, nil)
      end
    end

    def test_s_new_with_nonexistent_table
      ex = assert_raises DuckDB::Error do
        DuckDB::TableDescription.new(@con, 'nope')
      end
      refute_empty ex.message
    end

    def test_s_new_with_nonexistent_schema
      ex = assert_raises DuckDB::Error do
        DuckDB::TableDescription.new(@con, 't', schema: 'no_such_schema')
      end
      refute_empty ex.message
    end

    def test_s_new_with_nonexistent_catalog
      ex = assert_raises DuckDB::Error do
        DuckDB::TableDescription.new(@con, 't', catalog: 'no_such_catalog')
      end
      refute_empty ex.message
    end

    def test_error_message_returns_nil_on_success
      td = DuckDB::TableDescription.new(@con, 't')

      assert_nil td.error_message
    end

    def test_column_descriptions_return_array
      td = DuckDB::TableDescription.new(@con, 't')
      cds = td.column_descriptions

      assert_instance_of(Array, cds)
      assert_equal(2, cds.length)
    end

    def test_column_descriptions_return_array_of_column_description
      td = DuckDB::TableDescription.new(@con, 't')
      cds = td.column_descriptions

      assert_instance_of(DuckDB::ColumnDescription, cds[0])
      assert_instance_of(DuckDB::ColumnDescription, cds[1])
    end
  end
  end
end
