require 'test_helper'

module DuckDBTest
  class ResultBitTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
    end

    def test_result_bit
      @conn.execute('CREATE TABLE test (value BIT);')
      @conn.execute("INSERT INTO test VALUES ('1'::BIT);")
      @conn.execute("INSERT INTO test VALUES ('0101101'::BIT);")
      @conn.execute("INSERT INTO test VALUES ('0'::BIT);")
      @conn.execute("INSERT INTO test VALUES ('00000000'::BIT);")
      @conn.execute("INSERT INTO test VALUES ('1010101001'::BIT);")
      long_bits = '11111111111111111111111111111111111110101010101010101010101010101010101010101011100000000'
      @conn.execute("INSERT INTO test VALUES ('#{long_bits}'::BIT);")
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a
      assert_equal([["1"], ["0101101"], ["0"], ["00000000"], ["1010101001"], [long_bits]], ary)
    end

    def test_result_bit_nil
      @conn.execute('CREATE TABLE test (value BIT);')
      @conn.execute("INSERT INTO test VALUES (NULL);")
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a
      assert_equal([[nil]], ary)
    end

    def teardown
      @conn.execute('DROP TABLE test;')
      @conn.close
      @db.close
    end
  end
end

