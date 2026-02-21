# frozen_sring_literal: true

require 'duckdb'
require 'csv'

require 'stringio'

module DuckDB
  class Connection
    def register_as_table_with_table_function(name, io, csv_options: {})
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
          csv.rewind
          0
        end
      end
      register_table_function(tf)
    end

    def register_as_table_with_create_table(name, io, csv_options: {})
      csv = CSV.new(io, **csv_options)
      headers = csv.first.headers
      csv.rewind
      execute("CREATE OR REPLACE TABLE #{name} (#{headers.map { |h| "#{h} VARCHAR" }.join(', ')})")
      csv.each do |row|
        values = row.map { |cell| "'#{cell[1]}'" }.join(', ')
        execute("INSERT INTO #{name} VALUES (#{values})")
      end
    end
  end
end

csv_data = 'id,name,age'
csv_data += 100_000.times.map { |i| "\n#{i + 1},Name#{i + 1},#{rand(0..100)}" }.join
csv_io = StringIO.new(csv_data)

db = DuckDB::Database.open
con = db.connect
con.query('SET threads=1')

start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
con.register_as_table_with_table_function('csv_tf', csv_io, csv_options: { headers: true })
con.query('SELECT * FROM csv_tf()').to_a
end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

start_time2 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
con.register_as_table_with_create_table('csv_ct', csv_io, csv_options: { headers: true })
con.query('SELECT * FROM csv_ct').to_a
end_time2 = Process.clock_gettime(Process::CLOCK_MONOTONIC)

con.close
db.close

puts "Time taken for table function approach: #{end_time - start_time} seconds"
puts "Time taken for CREATE TABLE   approach: #{end_time2 - start_time2} seconds"
