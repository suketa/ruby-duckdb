# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ResultTimeTzTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
      @conn.execute('INSTALL icu;')
      @conn.execute('LOAD icu;')
    end

    def test_result_timestamp_tz_plus
      @conn.execute('SET TimeZone="Asia/Kabul";')
      @conn.execute('CREATE TABLE test (value TIMETZ);')
      @conn.execute("INSERT INTO test VALUES ('2019-01-02 12:34:56.123456789');")
      result = @conn.execute('SELECT value FROM test;')
      time = result.each.to_a.first.first
      assert_equal('12:34:56.123456+04:30', time.strftime('%H:%M:%S.%6N%:z'))
    end

    def test_result_timestamp_tz_minus
      @conn.execute('SET TimeZone="America/St_Johns";')
      @conn.execute('CREATE TABLE test (value TIMETZ);')
      @conn.execute("INSERT INTO test VALUES ('2019-01-02 12:34:56.123456789');")
      result = @conn.execute('SELECT value FROM test;')
      time = result.each.to_a.first.first
      assert_equal('12:34:56.123456-02:30', time.strftime('%H:%M:%S.%6N%:z'))
    end

    def teardown
      @conn.execute('DROP TABLE test;')
      @conn.close
      @db.close
    end
  end
end

