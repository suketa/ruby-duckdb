# frozen_string_literal: true

require 'test_helper'
require 'time'

module DuckDBTest
  class ExtractedStatementsTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
    end

    def teardown
      @con.close
      @db.close
    end

    def test_s_new
      assert_instance_of DuckDB::ExtractedStatements, DuckDB::ExtractedStatements.new(@con, 'SELECT 1')

      assert_raises TypeError do
        DuckDB::ExtractedStatements.new(1, 'SELECT 2')
      end

      assert_raises TypeError do
        DuckDB::ExtractedStatements.new(@con, 2)
      end
    end

    def test_s_new_with_invalid_sql
      ex = assert_raises DuckDB::Error do
        DuckDB::ExtractedStatements.new(@con, 'SELECT 1; INVALID STATEMENT; SELECT 3')
      end
      assert_match(/\AParser Error: syntax error at or near "INVALID"/, ex.message)
    end

    def test_size
      stmts = DuckDB::ExtractedStatements.new(@con, 'SELECT 1; SELECT 2; SELECT 3')

      assert_equal 3, stmts.size
    end

    def test_prepared_statement
      stmts = DuckDB::ExtractedStatements.new(@con, 'SELECT 1; SELECT 2; SELECT 3')
      stmt = stmts.prepared_statement(@con, 0)

      assert_instance_of(DuckDB::PreparedStatement, stmt)
      r = stmt.execute

      assert_equal([[1]], r.to_a)

      stmt = stmts.prepared_statement(@con, 1)
      r = stmt.execute

      assert_equal([[2]], r.to_a)

      stmt = stmts.prepared_statement(@con, 2)
      r = stmt.execute

      assert_equal([[3]], r.to_a)
    end

    def test_prepared_statement_with_invalid_index
      stmts = DuckDB::ExtractedStatements.new(@con, 'SELECT 1; SELECT 2; SELECT 3')
      ex = assert_raises DuckDB::Error do
        stmts.prepared_statement(@con, 3)
      end
      assert_equal 'Failed to create DuckDB::PreparedStatement object.', ex.message

      ex = assert_raises DuckDB::Error do
        stmts.prepared_statement(@con, -1)
      end
      assert_equal 'Failed to create DuckDB::PreparedStatement object.', ex.message
    end

    def test_destroy
      stmts = DuckDB::ExtractedStatements.new(@con, 'SELECT 1; SELECT 2; SELECT 3')

      assert_nil(stmts.destroy)
    end

    def test_each_without_block
      stmts = DuckDB::ExtractedStatements.new(@con, 'SELECT 1; SELECT 2; SELECT 3')
      enum_stmts = stmts.each

      assert_instance_of(Enumerator, enum_stmts)
      assert_equal(3, enum_stmts.size)
    end

    def test_each_with_block
      stmts = DuckDB::ExtractedStatements.new(@con, 'SELECT 1; SELECT 2; SELECT 3')
      i = 1
      stmts.each do |stmt|
        r = stmt.execute

        assert_equal([[i]], r.to_a)
        i += 1
      end
    end
  end
end
