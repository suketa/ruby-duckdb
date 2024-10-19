require 'duckdb'

temp_file = "#{File.expand_path('.', __dir__)}/data_issue775.duckdb"

file_last_saved = nil
DuckDB::Database.open(temp_file) do |db|
  db.connect do |con|
    con.execute("CREATE TABLE test (id INTEGER)")
    con.execute("INSERT INTO test VALUES (?), (?)", 1, 2)

    file_last_saved = File.mtime(temp_file)
  end
end
# GC.start
puts 'waiting for close' while file_last_saved == File.mtime(temp_file)

File.delete(temp_file)
