require 'test_helper'

module DuckDBTest
  class ConnectionTest < Minitest::Test
    def setup
      @con = DuckDB::Database.open.connect
    end

    def test_query
      assert_instance_of(DuckDB::Result, @con.query('CREATE TABLE table1 (id INTEGER)'))
    end

    def test_query_argument
      assert_raises(DuckDB::Error) { @con.query('foo', 'bar') }

      assert_raises(ArgumentError) { @con.query }

      assert_raises(TypeError) { @con.query(1) }

      assert_raises(DuckDB::Error) do
        invalid_sql = 'CREATE TABLE table1 ('
        @con.query(invalid_sql)
      end
    end
  end
end
