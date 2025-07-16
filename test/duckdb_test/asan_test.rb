require 'duckdb'
begin
  DuckDB::Database.open('not_exist_dir/not_exist_file')
rescue
  puts "Error: #{$!}"
end
