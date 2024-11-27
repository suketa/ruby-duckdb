# frozen_string_literal: true
require 'duckdb'

temp_file = "#{File.expand_path('.', __dir__)}/data.duckdb"
puts temp_file

db = DuckDB::Database.open(temp_file)
con = db.connect

con.query('CREATE TABLE test (id INTEGER)')
con.query('INSERT INTO test VALUES (?), (?)', 1, 2)

con.close
db.close

file_last_saved1 = File.mtime(temp_file)

i = 0
while file_last_saved1 == File.mtime(temp_file) && i < 5000
  sleep 0.0001
  i += 1
end

file_last_saved2 = File.mtime(temp_file)

p file_last_saved1
p file_last_saved2
File.delete(temp_file)
