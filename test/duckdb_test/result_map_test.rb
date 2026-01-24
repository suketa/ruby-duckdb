# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ResultMapTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
    end

    def teardown
      @conn.execute('DROP TABLE test;')
      @conn.close
      @db.close
    end

    def test_result_map
      @conn.execute('CREATE TABLE test (value MAP(INTEGER, INTEGER));')
      @conn.execute('INSERT INTO test VALUES (MAP{1: 2, 3: 4});')
      @conn.execute('INSERT INTO test VALUES (MAP{5: 6, 7: 8, 9: 10});')
      @conn.execute('INSERT INTO test VALUES (MAP{7: 8});')
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a

      assert_equal([[{ 1 => 2, 3 => 4 }], [{ 5 => 6, 7 => 8, 9 => 10 }], [{ 7 => 8 }]], ary)
    end

    def test_result_map_with_nil
      @conn.execute('CREATE TABLE test (value MAP(INTEGER, INTEGER));')
      @conn.execute('INSERT INTO test VALUES (MAP{1: 2, 3: 4});')
      @conn.execute('INSERT INTO test VALUES (MAP{5: NULL, 7: 8, 9: 10});')
      @conn.execute('INSERT INTO test VALUES (MAP{7: 8});')
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a

      assert_equal([[{ 1 => 2, 3 => 4 }], [{ 5 => nil, 7 => 8, 9 => 10 }], [{ 7 => 8 }]], ary)
    end

    def test_result_map_map
      @conn.execute('CREATE TABLE test (value MAP(INTEGER, MAP(INTEGER, INTEGER)));')
      @conn.execute('INSERT INTO test VALUES (MAP{1: MAP{2: 3, 4: 5}, 6: MAP{7: 8}});')
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a

      assert_equal([[{ 1 => { 2 => 3, 4 => 5 }, 6 => { 7 => 8 } }]], ary)
    end
  end
end
