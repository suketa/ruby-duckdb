require 'test_helper'
require 'tempfile'

module DuckDBTest
  class DatabaseTest < Minitest::Test
    def setup
      @path = create_path
    end

    def teardown
      File.unlink(@path) if File.exist?(@path)
      File.unlink(@path + '.wal') if File.exist?(@path + '.wal')
    end

    def test_s__open
      assert_raises(NoMethodError) { DuckDB::Database._open }
    end

    def test_s_open
      assert_instance_of(DuckDB::Database, DuckDB::Database.open)
    end

    def test_s_open_argument
      assert_instance_of(DuckDB::Database, DuckDB::Database.open(@path))
      assert_raises(ArgumentError) { DuckDB::Database.open('foo', 'bar') }
      assert_raises(TypeError) { DuckDB::Database.open(1) }

      assert_raises(DuckDB::Error) do
        not_exist_path = create_path + '/' + create_path
        DuckDB::Database.open(not_exist_path)
      end
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

      #
      # The error message is changed from DuckDB 0.2.4
      #
      assert(exception.message.include?('SELECT * from DUMMY') || exception.message.include?('has been closed'))
    end

    private

    def create_path
      Time.now.strftime('%Y%m%d%H%M%S') + '-' + Process.pid.to_s + '-' + (100..999).to_a.sample.to_s
    end
  end
end
