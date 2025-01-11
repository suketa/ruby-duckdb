# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class RubyAsanTest < Minitest::Test
    def test_with_ruby_asan
      db = DuckDB::Database.open
      con = db.connect
      con.execute('CREATE TABLE test (id INTEGER PRIMARY KEY, name VARCHAR(100))')
      con.execute("INSERT INTO test VALUES (1, 'hello'); INSERT INTO test VALUES (2, 'world')")
      # puts 'ExtractedStatements#each with PreapredStatement#(execute|destroy)'
      # stmts = DuckDB::ExtractedStatements.new(con, "INSERT INTO test VALUES (1, 'hello')")
      # stmts.each do |stmt|
      #   stmt.execute
      #   stmt.destroy
      # end
      result = con.execute('SELECT * FROM test WHERE id = ?', 1)
    end
  end
end
