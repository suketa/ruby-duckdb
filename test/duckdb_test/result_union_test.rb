# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ResultUnionTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
    end

    def test_result_union
      @conn.execute('CREATE TABLE test (u UNION(a INTEGER, b VARCHAR, c TIMESTAMP, d BIGINT));')
      @conn.execute("INSERT INTO test VALUES (1::INTEGER);")
      @conn.execute("INSERT INTO test VALUES ('abc'::VARCHAR);")
      @conn.execute("INSERT INTO test VALUES ('2020-01-01 00:00:00'::TIMESTAMP);")
      @conn.execute("INSERT INTO test VALUES (15::BIGINT);");

      # FIXME: support union and add assertions.
      result = @conn.execute('SELECT * FROM test;')
      ary = result.each.to_a
      p ary

      assert(ary.size > 0)
    end

    def teardown
      @conn.execute('DROP TABLE test;')
      @conn.close
      @db.close
    end
  end
end
