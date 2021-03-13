require 'test_helper'

module DuckDBTest
  class ConnectionTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
    end

    def test_query
      assert_instance_of(DuckDB::Result, @con.query('CREATE TABLE table1 (id INTEGER)'))
    end

    def test_query_with_valid_params
      @con.query('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      assert_instance_of(DuckDB::Result, @con.query('INSERT INTO t VALUES(?, ?)', 1, 'a'))
      r = @con.query('SELECT col1, col2 FROM t WHERE col1 = ? and col2 = ?', 1, 'a')
      assert_equal(1, r.each.first[0])
      assert_equal('a', r.each.first[1])
    end

    def test_query_with_invalid_params
      assert_raises(DuckDB::Error) { @con.query('foo', 'bar') }

      assert_raises(ArgumentError) { @con.query }

      assert_raises(TypeError) { @con.query(1) }

      assert_raises(DuckDB::Error) do
        invalid_sql = 'CREATE TABLE table1 ('
        @con.query(invalid_sql)
      end
    end

    def test_execute
      @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      assert_instance_of(DuckDB::Result, @con.execute('INSERT INTO t VALUES(?, ?)', 1, 'a'))
      r = @con.execute('SELECT col1, col2 FROM t WHERE col1 = ? and col2 = ?', 1, 'a')
      assert_equal(1, r.each.first[0])
      assert_equal('a', r.each.first[1])
    end

    def test_disconnect
      @con.disconnect
      exception = assert_raises(DuckDB::Error) do
        @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      end
      assert_equal('Database connection closed', exception.message)
    end

    def test_close
      @con.close
      exception = assert_raises(DuckDB::Error) do
        @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      end
      assert_equal('Database connection closed', exception.message)
    end

    def test_connect
      @con.disconnect
      exception = assert_raises(DuckDB::Error) do
        @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      end
      assert_equal('Database connection closed', exception.message)
      @con.connect(@db)
      assert_instance_of(DuckDB::Result, @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)'))
    end

    def test_connect_with_block
      @con.disconnect
      @con.connect(@db) do |con|
        assert_instance_of(DuckDB::Result, con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)'))
      end
      exception = assert_raises(DuckDB::Error) do
        @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      end
      assert_equal('Database connection closed', exception.message)
    end

    def test_open
      @con.disconnect
      exception = assert_raises(DuckDB::Error) do
        @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      end
      assert_equal('Database connection closed', exception.message)
      @con.open(@db)
      assert_instance_of(DuckDB::Result, @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)'))
    end

    def test_prepared_statement
      @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      assert_instance_of(DuckDB::PreparedStatement, @con.prepared_statement('SELECT * FROM t WHERE col1 = $1'))
    end
  end
end
