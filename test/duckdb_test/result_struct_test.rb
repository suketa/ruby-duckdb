require 'test_helper'

module DuckDBTest
  class ResultStructTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
    end

    def test_result_struct
      @conn.execute('CREATE TABLE test (s STRUCT(v VARCHAR, i INTEGER));')
      @conn.execute("INSERT INTO test VALUES (ROW('abc', 12));")
      @conn.execute("INSERT INTO test VALUES (ROW('de', 5));")
      result = @conn.execute('SELECT * FROM test;')
      ary = result.each.to_a
      assert_equal({ v: 'abc', i: 12 }, ary.first[0])
      assert_equal({ v: 'de', i: 5 }, ary.last[0])
    end

    def test_result_struct_with_nil
      @conn.execute('CREATE TABLE test (s STRUCT(v VARCHAR, i INTEGER));')
      @conn.execute("INSERT INTO test VALUES (ROW('abc', 12));")
      @conn.execute('INSERT INTO test VALUES (ROW(NULL, 5));')
      result = @conn.execute('SELECT * FROM test;')
      ary = result.each.to_a
      assert_equal({ v: 'abc', i: 12 }, ary.first[0])
      assert_equal({ v: nil, i: 5 }, ary.last[0])
    end

    def test_result_struct_with_key_having_space
      @conn.execute('CREATE TABLE test (s STRUCT("v 1" VARCHAR, i INTEGER));')
      @conn.execute("INSERT INTO test VALUES (ROW('abc', 12));")
      result = @conn.execute('SELECT * FROM test;')
      ary = result.each.to_a
      assert_equal({ 'v 1': 'abc', i: 12 }, ary.first[0])
    end

    def test_result_struct_struct
      @conn.execute('CREATE TABLE test (s STRUCT(v STRUCT(a VARCHAR, b INTEGER), i INTEGER));')
      @conn.execute("INSERT INTO test VALUES (ROW(ROW('abc', 12), 34));")
      result = @conn.execute('SELECT * FROM test;')
      ary = result.each.to_a
      assert_equal({ v: { a: 'abc', b: 12 }, i: 34 }, ary.first[0])
    end

    def teardown
      @conn.execute('DROP TABLE test;')
      @conn.close
      @db.close
    end
  end
end
