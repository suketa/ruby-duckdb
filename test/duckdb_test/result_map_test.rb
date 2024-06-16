require 'test_helper'

module DuckDBTest
  class ResultMapTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
    end

    def test_result_list
      @conn.execute('CREATE TABLE test (value MAP(INTEGER, INTEGER));')
      @conn.execute('INSERT INTO test VALUES (MAP{1: 2, 3: 4});')
      @conn.execute('INSERT INTO test VALUES (MAP{5: 6, 7: 8, 9: 10});')
      @conn.execute('INSERT INTO test VALUES (MAP{7: 8});')
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a
      p ary
    end

    def teardown
      @conn.execute('DROP TABLE test;')
      @conn.close
      @db.close
    end
  end
end

