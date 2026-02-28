# frozen_string_literal: true

require 'duckdb'
require 'polars-df'
require 'tmpdir'

class PolarsDataFrameTableAdapter
  def call(data_frame, name, columns: nil)
    columns ||= infer_columns(data_frame)
    DuckDB::TableFunction.create(name:, columns:, &execute_block(data_frame))
  end

  private

  def execute_block(data_frame)
    counter = 0
    height = data_frame.height
    width = data_frame.width
    proc do |_func_info, output|
      next counter = 0 if counter >= height

      write_row(data_frame, output, counter, width)
      counter += 1
      1
    end
  end

  def write_row(data_frame, output, counter, width)
    width.times { |index| output.set_value(index, 0, data_frame[counter, index]) }
  end

  def infer_columns(data_frame)
    data_frame.columns.to_h { |header| [header, DuckDB::LogicalType::VARCHAR] }
  end
end

def query_via_parquet(con, data_frame, name, parquet_path)
  data_frame.write_parquet(parquet_path)
  con.query("CREATE OR REPLACE TABLE #{name} AS SELECT * FROM read_parquet('#{parquet_path}')")
  con.query("SELECT * FROM #{name}").to_a
end

DuckDB::TableFunction.add_table_adapter(Polars::DataFrame, PolarsDataFrameTableAdapter.new)

df = Polars::DataFrame.new(
  {
    id: 100_000.times.map { |i| i + 1 },
    name: 100_000.times.map { |i| "Name#{i + 1}" },
    age: 100_000.times.map { rand(0..100) }
  }
)

db = DuckDB::Database.open
con = db.connect
con.query('SET threads=1')

start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
con.expose_as_table(df, 'polars_tf')
con.query('SELECT * FROM polars_tf()').to_a
end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

parquet_path = File.join(Dir.tmpdir, 'issue922_benchmark.parquet')
start_time2 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
query_via_parquet(con, df, 'polars_pq', parquet_path)
end_time2 = Process.clock_gettime(Process::CLOCK_MONOTONIC)

con.close
db.close
File.delete(parquet_path)

puts "Time taken for table function approach: #{end_time - start_time} seconds"
puts "Time taken for parquet file   approach: #{end_time2 - start_time2} seconds"
