# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class RubyAsanTest < Minitest::Test
    def test_with_ruby_asan
      db = DuckDB::Database.open
      con = db.connect
      con.execute('CREATE TABLE test (id INTEGER PRIMARY KEY, name VARCHAR(100))')
    end
  end
end
