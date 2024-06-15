require 'test_helper'

module DuckDBTest
  class ResultListTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
    end

    def test_result_list
      @conn.execute('CREATE TABLE test (value INTEGER[]);')
      @conn.execute('INSERT INTO test VALUES ([1, 2]);')
      @conn.execute('INSERT INTO test VALUES ([3, 4, 5]);')
      @conn.execute('INSERT INTO test VALUES ([6, 7, 8, 9]);')
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a
      assert_equal([[[1, 2]], [[3, 4, 5]], [[6, 7, 8, 9]]], ary)
    end

    def test_result_list_with_null
      @conn.execute('CREATE TABLE test (value INTEGER[]);')
      @conn.execute('INSERT INTO test VALUES ([1, 2]);')
      @conn.execute('INSERT INTO test VALUES ([3, 4, NULL]);')
      @conn.execute('INSERT INTO test VALUES ([6, 7, 8, 9]);')
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a
      assert_equal([[[1, 2]], [[3, 4, nil]], [[6, 7, 8, 9]]], ary)
    end

    def test_result_list_varchar
      @conn.execute('CREATE TABLE test (value VARCHAR[]);')
      @conn.execute("INSERT INTO test VALUES (['a', 'b']);")
      @conn.execute("INSERT INTO test VALUES (['c', 'd', 'e']);")
      @conn.execute("INSERT INTO test VALUES (['f', 'g', 'h', 'i']);")
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a
      assert_equal([[%w[a b]], [%w[c d e]], [%w[f g h i]]], ary)
    end

    def test_result_list_of_list
      @conn.execute('CREATE TABLE test (value INTEGER[][]);')
      @conn.execute('INSERT INTO test VALUES ([[1, 2, 3], [4, 5]]);')
      @conn.execute('INSERT INTO test VALUES ([[6], [7, 8], [9, 10]]);')
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a
      assert_equal([[[[1, 2, 3], [4, 5]]], [[[6], [7, 8], [9, 10]]]], ary)
    end


    def teardown
      @conn.execute('DROP TABLE test;')
      @conn.close
      @db.close
    end
  end
end
