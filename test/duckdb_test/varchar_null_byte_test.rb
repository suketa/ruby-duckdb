# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  # VARCHAR values used to be handed to DuckDB as a bare pointer, so an embedded
  # NUL ended them early: Ruby validated the whole String while the database
  # stored only the prefix.
  class VarcharNullByteTest < Minitest::Test
    EMBEDDED_NUL = "attacker@evil.test\0@corp.example"

    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      @con.query('CREATE TABLE t (v VARCHAR)')
    end

    def teardown
      @con&.close
      @db&.close
    end

    def stored
      @con.query('SELECT v FROM t').first.first
    end

    def test_query_binding_keeps_the_bytes_after_a_null_byte
      @con.query('INSERT INTO t VALUES (?)', EMBEDDED_NUL)

      assert_equal EMBEDDED_NUL, stored
    end

    def test_bind_varchar_keeps_the_bytes_after_a_null_byte
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO t VALUES (?)')
      stmt.bind_varchar(1, EMBEDDED_NUL)
      stmt.execute

      assert_equal EMBEDDED_NUL, stored
    end

    def test_appending_keeps_the_bytes_after_a_null_byte
      @con.appender('t') { |appender| appender.append_row(EMBEDDED_NUL) }

      assert_equal EMBEDDED_NUL, stored
    end

    def test_a_leading_null_byte_no_longer_stores_an_empty_string
      @con.query('INSERT INTO t VALUES (?)', "\0hidden")

      assert_equal "\0hidden", stored
    end
  end
end
