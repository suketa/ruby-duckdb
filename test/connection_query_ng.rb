# example1.rb
require 'duckdb'

temp_file = "#{File.expand_path('.', __dir__)}/data.duckdb"
puts temp_file

db = DuckDB::Database.open(temp_file)
con = db.connect

con.query('CREATE TABLE test (id INTEGER)')
con.query('INSERT INTO test VALUES (?), (?)', 1, 2)

con.close
db.close

file_last_saved1 = File.mtime(temp_file) # DBクローズ直後にファイルの最終更新日時を取得
i = 0
while file_last_saved1 == File.mtime(temp_file) && i < 5000
  sleep 0.0001
  i += 1
end
file_last_saved2 = File.mtime(temp_file) # しばらくしてからファイルの最終更新日時を取得

# なぜか file_last_saved1 と file_last_saved2 が異なる
p file_last_saved1 # => 2024-11-27 13:18:42.213986983 +0900 (1)
p file_last_saved2 # => 2024-11-27 13:18:42.643766951 +0900 (2)
File.delete(temp_file)
