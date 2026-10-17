# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ResultBitTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
    end

    def teardown
      @conn.execute('DROP TABLE test;')
      @conn.close
      @db.close
    end

    def test_result_bit
      @conn.execute('CREATE TABLE test (value BIT);')
      @conn.execute("INSERT INTO test VALUES ('1'::BIT);")
      @conn.execute("INSERT INTO test VALUES ('0101101'::BIT);")
      @conn.execute("INSERT INTO test VALUES ('0'::BIT);")
      @conn.execute("INSERT INTO test VALUES ('00000000'::BIT);")
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a

      assert_equal([['1'], ['0101101'], ['0'], ['00000000']], ary)
    end

    def test_result_bit_over_8_bits
      @conn.execute('CREATE TABLE test (value BIT);')
      over_8_bits = '1010101001'
      @conn.execute("INSERT INTO test VALUES ('#{over_8_bits}'::BIT);")
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a

      assert_equal([[over_8_bits]], ary)
    end

    def test_result_bit_long
      @conn.execute('CREATE TABLE test (value BIT);')
      long_bits = '11111111111111111111111111111111111110101010101010101010101010101010101010101011100000000'
      @conn.execute("INSERT INTO test VALUES ('#{long_bits}'::BIT);")
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a

      assert_equal([[long_bits]], ary)
    end

    def test_result_bit_nil
      @conn.execute('CREATE TABLE test (value BIT);')
      @conn.execute('INSERT INTO test VALUES (NULL);')
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a

      assert_equal([[nil]], ary)
    end

    def test_result_bit_with_invalid_padding
      create_table_with_invalid_bit
      result = @conn.execute('SELECT value FROM test;')

      error = assert_raises(DuckDB::Error) { result.each.to_a }

      assert_equal('invalid BIT value', error.message)
    end

    def test_result_bit_with_invalid_padding_in_nested_types
      create_table_with_invalid_bit

      [
        'SELECT [value, value] FROM test;',
        'SELECT [value, value]::BIT[2] FROM test;',
        'SELECT MAP {1: value} FROM test;',
        "SELECT {'a': value} FROM test;",
        'SELECT union_value(b := value) FROM test;'
      ].each do |sql|
        error = assert_raises(DuckDB::Error) { @conn.execute(sql).each.to_a }

        assert_equal('invalid BIT value', error.message)
      end
    end

    private

    def create_table_with_invalid_bit
      @conn.execute('CREATE TABLE test (value BIT);')
      stmt = @conn.prepare('INSERT INTO test VALUES (?);')
      stmt.bind_value(1, DuckDB::Value.send(:_create_bit, "\x80\x01".b))
      stmt.execute
    end
  end
end
