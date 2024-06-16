require 'test_helper'

module DuckDBTest
  class ResultListTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
    end

    def test_result_list
      @conn.execute('CREATE TABLE test (s STRUCT(v VARCHAR, i INTEGER));')
      @conn.execute("INSERT INTO test VALUES (ROW('abc', 12));")
      @conn.execute("INSERT INTO test VALUES (ROW('de', 5));")
      result = @conn.execute('SELECT * FROM test;')
      ary = result.each.to_a
      assert_equal({ v: 'abc', i: 12 }, ary.first[0])
      assert_equal({ v: 'de', i: 5 }, ary.last[0])
    end

    def teardown
      @conn.execute('DROP TABLE test;')
      @conn.close
      @db.close
    end
  end
end
