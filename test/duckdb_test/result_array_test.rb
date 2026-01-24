# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ResultArrayTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
    end

    def teardown
      @conn.execute('DROP TABLE test;')
      @conn.close
      @db.close
    end

    def test_result_array
      @conn.execute('CREATE TABLE test (value INTEGER[3]);')
      @conn.execute('INSERT INTO test VALUES (array_value(1, 2, 3));')
      @conn.execute('INSERT INTO test VALUES (array_value(4, 5, 6));')
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a

      assert_equal([[[1, 2, 3]], [[4, 5, 6]]], ary)
    end

    def test_result_array_with_null
      @conn.execute('CREATE TABLE test (value INTEGER[3]);')
      @conn.execute('INSERT INTO test VALUES (array_value(1, 2, 3));')
      @conn.execute('INSERT INTO test VALUES (array_value(4, 5, NULL));')
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a

      assert_equal([[[1, 2, 3]], [[4, 5, nil]]], ary)
    end

    def test_result_array_varchar
      @conn.execute('CREATE TABLE test (value varchar[3]);')
      @conn.execute("INSERT INTO test VALUES (array_value('abc', 'de', 'f'));")
      @conn.execute("INSERT INTO test VALUES (array_value('𝘶ñîҫȫ𝘥ẹ 𝖘ţ𝗋ⅰɲ𝓰 😃', 'あいうえお', '123'));")
      result = @conn.execute('SELECT value FROM test;')
      ary = result.each.to_a

      assert_equal([[%w[abc de f]], [['𝘶ñîҫȫ𝘥ẹ 𝖘ţ𝗋ⅰɲ𝓰 😃', 'あいうえお', '123']]], ary)
    end
  end
end
