# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ResultUnionTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
    end

    def teardown
      @conn.execute('DROP TABLE test;')
      @conn.close
      @db.close
    end

    def test_result_union
      @conn.execute('CREATE TABLE test (u UNION(a INTEGER, b VARCHAR, c TIMESTAMP, d BIGINT));')
      @conn.execute('INSERT INTO test VALUES (1::INTEGER);')
      @conn.execute("INSERT INTO test VALUES ('abc'::VARCHAR);")
      @conn.execute("INSERT INTO test VALUES ('2020-01-01 00:00:00'::TIMESTAMP);")
      @conn.execute('INSERT INTO test VALUES (2::BIGINT);')

      result = @conn.execute('SELECT * FROM test;')
      ary = result.each.to_a

      assert_equal([[1], ['abc'], [Time.local(2020, 1, 1)], [2]], ary)
    end

    def test_result_union_with_null
      @conn.execute('CREATE TABLE test (u UNION(a INTEGER, b VARCHAR, c TIMESTAMP, d BIGINT));')
      @conn.execute('INSERT INTO test VALUES (1::INTEGER);')
      @conn.execute('INSERT INTO test VALUES (NULL);')
      result = @conn.execute('SELECT * FROM test;')
      ary = result.each.to_a

      assert_equal([[1], [nil]], ary)
    end
  end
end
