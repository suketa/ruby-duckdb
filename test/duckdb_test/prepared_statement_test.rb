require 'test_helper'

module DuckDBTest
  class PreparedStatementTest < Minitest::Test
    def self.create_table
      con = DuckDB::Database.open.connect
      con.query('CREATE TABLE a (id INTEGER)')
      con
    end

    def setup
      @@con ||= PreparedStatementTest.create_table
    end

    def test_class_exist
      assert_instance_of(Class, DuckDB::PreparedStatement)
    end

    def test_s_new
      assert_instance_of(DuckDB::PreparedStatement, DuckDB::PreparedStatement.new(@@con, 'SELECT * FROM a'))
      assert_raises(ArgumentError) { DuckDB::PreparedStatement.new(@@con) }
      assert_raises(ArgumentError) { DuckDB::PreparedStatement.new }
      assert_raises(TypeError) { DuckDB::PreparedStatement.new(@@con, 1) }
      assert_raises(TypeError) {
        DuckDB::PreparedStatement.new(1, 1)
      }
    end
  end
end
