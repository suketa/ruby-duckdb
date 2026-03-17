# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class MysqlTest < Minitest::Test
    def setup
      skip 'Set MYSQL_TEST environment variable to run MySQL tests' unless ENV['MYSQL_TEST']

      @db = DuckDB::Database.open
      @conn = @db.connect
      @conn.execute('INSTALL mysql;')
      @conn.execute('LOAD mysql;')
      @conn.execute(<<~ATTACH)
        ATTACH 'host=127.0.0.1 user=test_user password=test_password database=test_db port=3306'
        AS mysql_db (TYPE mysql);
      ATTACH
    end

    def teardown
      return unless @conn

      @conn.close
      @db.close
    end

    def test_mysql_query
      result = @conn.execute("SELECT * FROM mysql_query('mysql_db', 'select 1');")
      rows = result.each.to_a

      assert_equal(1, rows.length)
      assert_equal(1, rows.first.first)
    end
  end
end
