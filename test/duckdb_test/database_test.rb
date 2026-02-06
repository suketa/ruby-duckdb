# frozen_string_literal: true

require 'test_helper'
require 'tempfile'

module DuckDBTest
  class DatabaseTest < Minitest::Test
    def setup
      @path = create_path
    end

    def teardown
      FileUtils.rm_f(@path)
      walf = "#{@path}.wal"
      FileUtils.rm_f(walf)
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
    end

    def test_s_open_with_invalid_types
      assert_raises(TypeError) { DuckDB::Database.open('foo', 'bar') }
      assert_raises(TypeError) { DuckDB::Database.open(1) }
    end

    def test_s_open_with_nonexistent_path
      not_exist_path = "#{create_path}/#{create_path}"

      assert_raises(DuckDB::Error) do
        DuckDB::Database.open(not_exist_path)
      end
    end

    def test_s_open_with_config
      config = DuckDB::Config.new
      db = DuckDB::Database.open(nil, config)
      conn = db.connect
      r = conn.execute("SELECT current_setting('access_mode') AS access_mode;")

      assert_equal('automatic', r.first.first)

      config['access_mode'] = 'read_write'
      db = DuckDB::Database.open(nil, config)
      conn = db.connect
      r = conn.execute("SELECT current_setting('access_mode') AS access_mode;")

      assert_equal('read_write', r.first.first)
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
