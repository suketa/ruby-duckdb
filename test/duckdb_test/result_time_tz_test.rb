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

    def teardown
      @conn.execute('DROP TABLE test;')
      @conn.close
      @db.close
    end

    def test_result_timestamp_tz
      @conn.execute('SET TimeZone="Asia/Tokyo";')
      @conn.execute('CREATE TABLE test (value TIMETZ);')
      @conn.execute("INSERT INTO test VALUES ('2019-01-02 12:34:56.123456789');")
      result = @conn.execute('SELECT value FROM test;')
      time = result.each.to_a.first.first

      assert_equal('12:34:56.123456+09:00', time.strftime('%H:%M:%S.%6N%:z'))
    end
  end
end
