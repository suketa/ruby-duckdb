require 'test_helper'

module DuckDBTest
  class ResultBitTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
    end

    def test_result_list
      @conn.execute('CREATE TABLE test (value BIT);')
      @conn.execute("INSERT INTO test VALUES ('1'::BIT);")
      @conn.execute("INSERT INTO test VALUES ('0101101'::BIT);")
      @conn.execute("INSERT INTO test VALUES ('0'::BIT);")
      @conn.execute("INSERT INTO test VALUES ('00000000'::BIT);")
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

