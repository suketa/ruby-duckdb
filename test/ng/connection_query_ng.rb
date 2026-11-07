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

# Poll for up to 1 second of wall-clock time rather than a fixed number of
# iterations. Ruby master on Windows sleeps ~8ms for `sleep 0.0001`, which
# turned 5000 iterations into 40s+ and blew the CI test-step timeout.
deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 1.0
while file_last_saved1 == File.mtime(temp_file) &&
      Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
  sleep 0.0001
end

file_last_saved2 = File.mtime(temp_file)

p file_last_saved1
p file_last_saved2
File.delete(temp_file)
