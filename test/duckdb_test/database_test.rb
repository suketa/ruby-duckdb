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

    def test_s_open
      assert_instance_of(DuckDB::Database, DuckDB::Database.open)
    end

    def test_s_open_argument
      db = DuckDB::Database.open(@path)

      assert_instance_of(DuckDB::Database, db)
      db.close
    end

    def test_s_open_type_errors
      assert_raises(TypeError) { DuckDB::Database.open(1) }
    end

    def test_s_open_invalid_path
      not_exist_path = "#{create_path}/#{create_path}"

      assert_raises(DuckDB::Error) { DuckDB::Database.open(not_exist_path) }
    end

    def test_s_open_with_config
      config = DuckDB::Config.new
      db = DuckDB::Database.open(config: config)
      conn = db.connect
      r = conn.execute("SELECT current_setting('access_mode') AS access_mode;")

      assert_equal('automatic', r.first.first)

      config['access_mode'] = 'read_write'
      db = DuckDB::Database.open(config: config)
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

    def test_s_open_with_path_file
      db = DuckDB::Database.open(@path)

      assert_instance_of(DuckDB::Database, db)

      con = db.connect
      con.query('CREATE TABLE t (id INTEGER)')
      con.query('INSERT INTO t VALUES (1)')
      db.close

      db2 = DuckDB::Database.open(@path)
      result = db2.connect.query('SELECT * FROM t')

      assert_equal([[1]], result.to_a)
      db2.close
    end

    def test_s_open_with_block
      result = DuckDB::Database.open(:memory) do |db|
        assert_instance_of(DuckDB::Database, db)

        con = db.connect
        con.query('CREATE TABLE t (id INTEGER)')
      end

      assert_instance_of(DuckDB::Result, result)
    end

    def test_s_open_with_config_keyword
      config = DuckDB::Config.new
      config['access_mode'] = 'read_write'
      db = DuckDB::Database.open(config: config)
      con = db.connect
      result = con.execute("SELECT current_setting('access_mode') AS access_mode;")

      assert_equal('read_write', result.first.first)
      db.close
    end

    def test_s_open_with_memory_symbol
      db = DuckDB::Database.open(:memory)

      assert_instance_of(DuckDB::Database, db)

      con = db.connect

      assert_instance_of(DuckDB::Connection, con)
      db.close
    end

    def test_s_open_with_positional_config_deprecated
      config = DuckDB::Config.new
      config['access_mode'] = 'read_write'
      db = nil

      assert_output(nil, /deprecated/) { db = DuckDB::Database.open(nil, config) }

      assert_instance_of(DuckDB::Database, db)
      con = db.connect
      result = con.execute("SELECT current_setting('access_mode') AS access_mode;")

      assert_equal('read_write', result.first.first)
      db.close
    end

    def test_s_open_with_path_and_positional_config_deprecated
      config = DuckDB::Config.new
      db = nil

      assert_output(nil, /deprecated/) { db = DuckDB::Database.open(@path, config) }

      assert_instance_of(DuckDB::Database, db)
      db.close
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
