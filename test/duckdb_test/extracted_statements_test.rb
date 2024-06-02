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

    def teardown
      @con.close
      @db.close
    end
  end
end
