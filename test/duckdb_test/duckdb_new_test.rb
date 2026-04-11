# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class DuckDBDatabaseNewTest < Minitest::Test
    def setup
      @path = "#{Time.now.strftime('%Y%m%d%H%M%S')}-#{Process.pid}-#{rand(100..999)}"
    end

    def teardown
      FileUtils.rm_f(@path)
      FileUtils.rm_f("#{@path}.wal")
    end

    def test_database_new
      db = DuckDB::Database.new

      assert_instance_of(DuckDB::Database, db)

      con = db.connect

      assert_instance_of(DuckDB::Connection, con)
      db.close
    end

    def test_database_new_with_memory_symbol
      db = DuckDB::Database.new(:memory)

      assert_instance_of(DuckDB::Database, db)

      con = db.connect

      assert_instance_of(DuckDB::Connection, con)
      db.close
    end

    def test_database_new_with_config
      config = DuckDB::Config.new
      config['access_mode'] = 'read_write'
      db = DuckDB::Database.new(config: config)
      con = db.connect
      result = con.execute("SELECT current_setting('access_mode') AS access_mode;")

      assert_equal('read_write', result.first.first)
      db.close
    end

    def test_database_new_with_invalid_path_type
      assert_raises(TypeError) { DuckDB::Database.new(123) }
    end

    def test_database_new_with_invalid_path_symbol
      assert_raises(ArgumentError) { DuckDB::Database.new(:foo) }
    end

    def test_database_new_with_invalid_config
      assert_raises(TypeError) { DuckDB::Database.new(config: 'bad') }
    end

    def test_database_new_with_invalid_path_directory
      not_exist = "#{@path}/#{@path}"

      assert_raises(DuckDB::Error) { DuckDB::Database.new(not_exist) }
    end

    def test_database_new_with_block
      db = DuckDB::Database.new do |d|
        assert_instance_of(DuckDB::Database, d)

        con = d.connect
        con.query('CREATE TABLE t (id INTEGER)')
      end

      assert_instance_of(DuckDB::Database, db)
    end

    def test_database_new_with_path
      db = DuckDB::Database.new(@path)

      assert_instance_of(DuckDB::Database, db)
      db.close
    end

    def test_database_new_with_path_persists_data
      db = DuckDB::Database.new(@path)
      con = db.connect
      con.query('CREATE TABLE t (id INTEGER)')
      con.query('INSERT INTO t VALUES (42)')
      db.close

      db2 = DuckDB::Database.new(@path)
      result = db2.connect.query('SELECT * FROM t')

      assert_equal([[42]], result.to_a)
      db2.close
    end
  end
end
