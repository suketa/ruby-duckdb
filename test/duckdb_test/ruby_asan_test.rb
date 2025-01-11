# frozen_string_literal: true
require 'duckdb'

def run_duckdb_asan_test
  db = DuckDB::Database.open
  con = db.connect('abc')
  # con.execute('INSERT INTO test VALUES (?, "hello")', 1)
rescue DuckDB::Error => e
  p e
end

run_duckdb_asan_test
