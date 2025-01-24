# frozen_string_literal: true
require 'duckdb'

def run_duckdb_asan_test
  db = DuckDB::Database.open
  con = db.connect
  stmt = DuckDB::PreparedStatement.new(con, 'INSERT INTO test VALUES (?, "hello")')
rescue Exception => e
  p e
end

run_duckdb_asan_test
