require 'test_helper'

module DuckDBTest
  class PreparedStatementTest < Minitest::Test
    def self.create_table
      con = DuckDB::Database.open.connect
      con.query('CREATE TABLE a (id INTEGER, col_boolean BOOLEAN, col_varchar VARCHAR, col_date DATE, col_timestamp TIMESTAMP)')
      con.query("INSERT INTO a VALUES (1, True, 'str', '2019-11-09', '2019-11-09 12:34:56')")
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
      assert_raises(DuckDB::Error) { DuckDB::PreparedStatement.new(con, 'SELECT * FROM') }
    end

    def test_execute
      con = PreparedStatementTest.con
      stmt = DuckDB::PreparedStatement.new(con, 'SELECT * FROM a')
      result = stmt.execute
      assert_instance_of(DuckDB::Result, result)

      stmt = DuckDB::PreparedStatement.new(con, 'SELECT * FROM a where id = ?')
      assert_raises(DuckDB::Error) { stmt.execute }
    end

    def test_nparams
      con = PreparedStatementTest.con
      stmt = DuckDB::PreparedStatement.new(con, 'SELECT * FROM a')
      assert_equal(0, stmt.nparams)

      stmt = DuckDB::PreparedStatement.new(con, 'SELECT * FROM a WHERE id = $1')
      assert_equal(1, stmt.nparams)

      stmt = DuckDB::PreparedStatement.new(con, 'SELECT * FROM a WHERE id = ?')
      assert_equal(1, stmt.nparams)
    end

    def test_bind_boolean
      con = PreparedStatementTest.con
      stmt = DuckDB::PreparedStatement.new(con, 'SELECT * FROM a WHERE col_boolean = $1')

      assert_raises(ArgumentError) { stmt.bind_boolean(0, true) }
      assert_raises(DuckDB::Error) { stmt.bind_boolean(2, true) }

      stmt.bind_boolean(1, true)
      assert_equal(1, stmt.execute.each.size)

      stmt.bind_boolean(1, false)
      assert_equal(0, stmt.execute.each.size)

      assert_raises(ArgumentError) { stmt.bind_boolean(1, 'True') }
    end

    def test_bind_varchar
      con = PreparedStatementTest.con
      stmt = DuckDB::PreparedStatement.new(con, 'SELECT * FROM a WHERE col_varchar = $1')

      assert_raises(ArgumentError) { stmt.bind_varchar(0, 'str') }
      assert_raises(DuckDB::Error) { stmt.bind_varchar(2, 'str') }
      stmt.bind_varchar(1, 'str')
      result = stmt.execute
      assert_equal(1, result.each.first[0])

      stmt = DuckDB::PreparedStatement.new(con, 'SELECT * FROM a WHERE col_varchar = ?')

      assert_raises(ArgumentError) { stmt.bind_varchar(0, 'str') }
      assert_raises(DuckDB::Error) { stmt.bind_varchar(2, 'str') }

      stmt.bind_varchar(1, 'str')
      result = stmt.execute
      assert_equal(1, result.each.first[0])

      # SQL injection
      param = "' or 1 = 1 --"
      result = con.query("SELECT * FROM a WHERE col_varchar = '#{param}'")
      assert_equal(1, result.each.size)

      # block SQL injection using bind_varchar
      stmt.bind_varchar(1, param)
      result = stmt.execute
      assert_equal(0, result.each.size)
    end

    def test_bind_varchar_date
      con = PreparedStatementTest.con
      stmt = DuckDB::PreparedStatement.new(con, 'SELECT * FROM a WHERE col_date = $1')

      stmt.bind_varchar(1, '2019/11/09')
      result = stmt.execute
      assert_equal(1, result.each.first[0])

      stmt.bind_varchar(1, 'invalid_date_string')
      assert_raises(DuckDB::Error) { stmt.execute }
    end

    def test_bind_varchar_timestamp
      con = PreparedStatementTest.con
      stmt = DuckDB::PreparedStatement.new(con, 'SELECT * FROM a WHERE col_timestamp = $1')

      stmt.bind_varchar(1, '2019/11/09 12:34:56')
      result = stmt.execute
      assert_equal(1, result.each.first[0])

      stmt.bind_varchar(1, 'invalid_timestamp_string')
      assert_raises(DuckDB::Error) { stmt.execute }
    end
  end
end
