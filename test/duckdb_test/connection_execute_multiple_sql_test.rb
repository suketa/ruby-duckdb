# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ConnectionExecuteMultipleSqlTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
    end

    def test_multiple_sql
      exception = assert_raises(DuckDB::Error) do
        @con.execute('CREATE TABLE test (v VARCHAR); CREATE TABLE test (v VARCHAR); SELECT 42;')
      end
      assert_match(/Table with name "test" already exists/, exception.message)
    end

    def test_multiple_select_sql
      @con.execute('CREATE TABLE test (i INTEGER)')
      result = @con.execute(<<-SQL)
        INSERT INTO test VALUES (1), (2);
        SELECT * FROM test;
        INSERT INTO test VALUES (3), (4);
        SELECT * FROM test;
      SQL
      assert_equal([[1], [2], [3], [4]], result.to_a)
    end
  end
end
