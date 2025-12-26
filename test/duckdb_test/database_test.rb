# frozen_string_literal: true

require 'test_helper'
require 'tempfile'

module DuckDBTest
  class DatabaseTest < Minitest::Test
    def setup
      @path = create_path
    end

    def teardown
      File.unlink(@path) if File.exist?(@path)
      walf = "#{@path}.wal"
      File.unlink(walf) if File.exist?(walf)
    end

    def test_s__open
      assert_raises(NoMethodError) { DuckDB::Database._open }
    end

    def test_s_open
      assert_instance_of(DuckDB::Database, DuckDB::Database.open)
    end

    def test_s_open_argument
      db = DuckDB::Database.open(@path)
      assert_instance_of(DuckDB::Database, db)
      db.close

      assert_raises(TypeError) { DuckDB::Database.open('foo', 'bar') }
      assert_raises(TypeError) { DuckDB::Database.open(1) }

      assert_raises(DuckDB::Error) do
        not_exist_path = "#{create_path}/#{create_path}"
        DuckDB::Database.open(not_exist_path)
      end
    end

    def test_s_open_with_config
      library_version = Gem::Version.new(DuckDB::LIBRARY_VERSION)
      skip 'config test' if %w[1.4.0 1.4.1 1.4.2 1.4.3].include?(library_version.to_s)

      config = DuckDB::Config.new
      config['default_order'] = 'DESC'
      db = DuckDB::Database.open(nil, config)
      conn = db.connect
      conn.execute('CREATE TABLE t (col1 INTEGER);')
      conn.execute('INSERT INTO t VALUES(3),(1),(4),(2);')
      r = conn.execute('SELECT * FROM t ORDER BY col1')
      assert_equal([4], r.first)

      config['default_order'] = 'ASC'
      db = DuckDB::Database.open(nil, config)
      conn = db.connect
      conn.execute('CREATE TABLE t (col1 INTEGER);')
      conn.execute('INSERT INTO t VALUES(3),(1),(4),(2);')
      r = conn.execute('SELECT * FROM t ORDER BY col1')
      assert_equal([1], r.first)
    end

    def test_s_open_block
      result = DuckDB::Database.open do |db|
        assert_instance_of(DuckDB::Database, db)
        con = db.connect
        assert_instance_of(DuckDB::Connection, con)
        con.query('CREATE TABLE t (id INTEGER)')
      end
      assert_instance_of(DuckDB::Result, result)
    end

    def test_connect
      assert_instance_of(DuckDB::Connection, DuckDB::Database.open.connect)
    end

    def test_connect_with_block
      result = DuckDB::Database.open do |db|
        db.connect do |con|
          assert_instance_of(DuckDB::Connection, con)
          con.query('CREATE TABLE t (id INTEGER)')
        end
      end
      assert_instance_of(DuckDB::Result, result)
    end

    def test_close
      db = DuckDB::Database.open
      con = db.connect
      db.close
      exception = assert_raises(DuckDB::Error) do
        con.query('SELECT * from DUMMY')
      end

      assert_match(/DUMMY does not exist/, exception.message)
    end

    private

    def create_path
      "#{Time.now.strftime('%Y%m%d%H%M%S')}-#{Process.pid}-#{rand(100..999)}"
    end
  end
end
