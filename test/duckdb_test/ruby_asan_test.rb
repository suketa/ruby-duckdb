# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class RubyAsanTest < Minitest::Test
    def test_with_ruby_asan
      db = DuckDB::Database.open
      con = db.connect
      con.execute('CREATE TABLE test (id INTEGER PRIMARY KEY, name VARCHAR(100))')
      puts 'ExtractedStatements#each'
      stmts = DuckDB::ExtractedStatements.new(con, "INSERT INTO test VALUES (1, 'hello')")
      stmts.each do |stmt|
        stmt.execute
      end
      # result = con.execute('SELECT * FROM test WHERE id = ?', 1)
    end
  end
end
