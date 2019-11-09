require 'test_helper'

module DuckDBTest
  class PreparedStatementTest < Minitest::Test
    def self.create_table
      con = DuckDB::Database.open.connect
      con.query('CREATE TABLE a (id INTEGER)')
      con
    end

    def self.con
      @con ||= create_table
    end

    def test_class_exist
      assert_instance_of(Class, DuckDB::PreparedStatement)
    end

    def test_s_new
      con = PreparedStatementTest.con
      assert_instance_of(DuckDB::PreparedStatement, DuckDB::PreparedStatement.new(con, 'SELECT * FROM a'))
      assert_raises(ArgumentError) { DuckDB::PreparedStatement.new(con) }
      assert_raises(ArgumentError) { DuckDB::PreparedStatement.new }
      assert_raises(TypeError) { DuckDB::PreparedStatement.new(con, 1) }
      assert_raises(TypeError) { DuckDB::PreparedStatement.new(1, 1) }
    end

    def test_execute
      con = PreparedStatementTest.con
      stmt = DuckDB::PreparedStatement.new(con, 'SELECT * FROM a')
      result = stmt.execute
      assert_instance_of(DuckDB::Result, result)
    end
  end
end
