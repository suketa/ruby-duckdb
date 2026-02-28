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

DuckDB::TableFunction.add_table_adapter(CSV, CSVTableAdapter.new)

csv_data = "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35"
csv_data += 30_000.times.map { |i| "\n#{i + 4},Name#{i + 4},#{rand(0..100)}" }.join
csv = CSV.new(StringIO.new(csv_data), headers: true)

db = DuckDB::Database.open
con = db.connect
con.query('SET threads=1')
con.expose_as_table(csv, 'csv_table')
result = con.query('SELECT * FROM csv_table()').to_a

p result
puts result.first.first == '1'
puts result.first[1] == 'Alice'
