# frozen_sring_literal: true

require 'duckdb'
require 'csv'

require 'stringio'

module DuckDB
  class Connection
    def register_as_table(name, io, csv_options: {})
      csv = CSV.new(io, **csv_options)
      headers = csv.first.headers
      csv.rewind
      columns = headers.each_with_object({}) { |header, hash| hash[header] = LogicalType::VARCHAR }
      tf = DuckDB::TableFunction.create(
        name:,
        columns:
      ) do |_func_info, output|
        line = csv.readline
        if line
          line.each_with_index do |cell, index|
            output.set_value(index, 0, cell[1])
          end
          1
        else
          0
        end
      end
      register_table_function(tf)
    end
  end
end

db = DuckDB::Database.open

csv_data = "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35"
csv_data += 30000.times.map { |i| "\n#{i + 4},Name#{i + 4},#{rand(0..100)}" }.join
csv_io = StringIO.new(csv_data)

con = db.connect
con.query('SET threads=1')
con.register_as_table('csv_io', csv_io, csv_options: { headers: true })
result = con.query("SELECT * FROM csv_io()").to_a

p result
puts result.first.first == '1'
puts result.first[1] == 'Alice'
