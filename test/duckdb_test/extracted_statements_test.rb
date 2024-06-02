require 'test_helper'
require 'time'

module DuckDBTest
  class ExtractedStatementsTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
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

    def teardown
      @con.close
      @db.close
    end
  end
end
