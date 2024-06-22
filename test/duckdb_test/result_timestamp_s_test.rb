# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ResultTimestampSTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
    end

    def test_result_timestamp_s
      @conn.execute('CREATE TABLE test (value TIMESTAMP_S);')
      @conn.execute("INSERT INTO test VALUES ('2019-01-02 12:34:56.123456789');")
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a
      assert_equal([[Time.local(2019, 1, 2, 12, 34, 56)]], ary)
    end

    def teardown
      @conn.execute('DROP TABLE test;')
      @conn.close
      @db.close
    end
  end
end
