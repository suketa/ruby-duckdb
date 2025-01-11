# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class RubyAsanTest < Minitest::Test
    def test_with_ruby_asan
      db = DuckDB::Database.open
      con = db.connect
      assert_raises(DuckDB::Error) { con.execute('INSERT INTO test VALUES (?, "hello")', 1) }
    end
  end
end
