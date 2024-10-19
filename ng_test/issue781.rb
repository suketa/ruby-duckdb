require 'duckdb'

for wal_size in 9..2000
  puts "WAL Size: #{wal_size}"
  temp_file = "#{File.expand_path('.', __dir__)}/data.duckdb"

  db = DuckDB::Database.open(temp_file)
  con = db.connect

  con.execute("SET checkpoint_threshold='#{wal_size}.0 B'")

  con.execute("CREATE TABLE test (id FLOAT)")

  for i in 1..5000
    con.execute("INSERT INTO test VALUES (?), (?)", 1, 2)
  end

  con.execute('CHECKPOINT')

  con.close
  db.close
  GC.start

  File.delete(temp_file)
end
