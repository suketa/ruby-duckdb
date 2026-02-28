# frozen_string_literal: true

require 'duckdb'
require 'csv'
require 'stringio'

class CSVTableAdapter
  def call(csv, name, columns: nil)
    columns ||= infer_columns(csv)
    DuckDB::TableFunction.create(name:, columns:) do |_func_info, output|
      write_row(csv, output)
    end
  end

  private

  def write_row(csv, output)
    line = csv.readline
    if line
      line.each_with_index { |cell, index| output.set_value(index, 0, cell[1]) }
      1
    else
      csv.rewind
      0
    end
  end

  def infer_columns(csv)
    headers = csv.first.headers
    csv.rewind
    headers.to_h { |header| [header, DuckDB::LogicalType::VARCHAR] }
  end
end

def register_as_table_with_create_table(con, csv, name)
  headers = csv.first.headers
  csv.rewind
  con.execute("CREATE OR REPLACE TABLE #{name} (#{headers.map { |h| "#{h} VARCHAR" }.join(', ')})")
  csv.each do |row|
    values = row.map { |cell| "'#{cell[1]}'" }.join(', ')
    con.execute("INSERT INTO #{name} VALUES (#{values})")
  end
end

DuckDB::TableFunction.add_table_adapter(CSV, CSVTableAdapter.new)

csv_data = 'id,name,age'
csv_data += 100_000.times.map { |i| "\n#{i + 1},Name#{i + 1},#{rand(0..100)}" }.join

db = DuckDB::Database.open
con = db.connect
con.query('SET threads=1')

csv = CSV.new(StringIO.new(csv_data), headers: true)
start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
con.expose_as_table(csv, 'csv_tf')
con.query('SELECT * FROM csv_tf()').to_a
end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

csv2 = CSV.new(StringIO.new(csv_data), headers: true)
start_time2 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
register_as_table_with_create_table(con, csv2, 'csv_ct')
con.query('SELECT * FROM csv_ct').to_a
end_time2 = Process.clock_gettime(Process::CLOCK_MONOTONIC)

con.close
db.close

puts "Time taken for table function approach: #{end_time - start_time} seconds"
puts "Time taken for CREATE TABLE   approach: #{end_time2 - start_time2} seconds"
