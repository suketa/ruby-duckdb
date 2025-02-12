# frozen_string_literal: true
require 'duckdb'

def run_duckdb_asan_test
  db = DuckDB::Database.open
  con = db.connect
  DuckDB::PreparedStatement.new(con, 'INSERT INTO test VALUES (?, "hello")')
rescue StandardError => e
  p e
end

run_duckdb_asan_test
