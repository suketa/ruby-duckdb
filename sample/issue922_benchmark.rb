# frozen_string_literal: true

# rubocop:disable Style/OneClassPerFile
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

# Batch approach: write BATCH_SIZE rows per execute call to reduce Ruby<->C crossings
class PolarsDataFrameBatchTableAdapter
  BATCH_SIZE = 2048

  def call(data_frame, name, columns: nil)
    columns ||= infer_columns(data_frame)
    DuckDB::TableFunction.create(name:, columns:, &execute_block(data_frame))
  end

  private

  def execute_block(data_frame)
    offset = 0
    height = data_frame.height
    width = data_frame.width
    proc do |_func_info, output|
      next offset = 0 if offset >= height

      rows = [height - offset, BATCH_SIZE].min
      write_batch(data_frame, output, offset, rows, width)
      offset += rows
      rows
    end
  end

  def write_batch(data_frame, output, offset, rows, width)
    rows.times do |row_idx|
      width.times { |col_idx| output.set_value(col_idx, row_idx, data_frame[offset + row_idx, col_idx]) }
    end
  end

  def infer_columns(data_frame)
    data_frame.columns.to_h { |header| [header, DuckDB::LogicalType::VARCHAR] }
  end
end

# Optimized batch approach: pre-extract columns as Ruby arrays to avoid
# repeated Polars FFI calls, and use assign_string_element to skip type dispatch
class PolarsDataFrameOptimizedTableAdapter
  BATCH_SIZE = 2048

  def call(data_frame, name, columns: nil)
    columns ||= infer_columns(data_frame)
    DuckDB::TableFunction.create(name:, columns:, &execute_block(data_frame))
  end

  private

  # rubocop:disable Metrics/MethodLength
  def execute_block(data_frame)
    col_arrays = extract_columns(data_frame)
    offset = 0
    height = data_frame.height
    width = data_frame.width
    proc do |_func_info, output|
      next offset = 0 if offset >= height

      rows = [height - offset, BATCH_SIZE].min
      vectors = width.times.map { |i| output.get_vector(i) }
      write_batch(col_arrays, vectors, offset, rows)
      offset += rows
      rows
    end
  end
  # rubocop:enable Metrics/MethodLength

  def extract_columns(data_frame)
    data_frame.columns.map { |col| data_frame[col].cast(Polars::Utf8).to_a }
  end

  def write_batch(col_arrays, vectors, offset, rows)
    col_arrays.each_with_index do |col_data, col_idx|
      vec = vectors[col_idx]
      rows.times { |row_idx| vec.assign_string_element(row_idx, col_data[offset + row_idx].to_s) }
    end
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

DuckDB::TableFunction.add_table_adapter(Polars::DataFrame, PolarsDataFrameTableAdapter.new)
start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
con.expose_as_table(df, 'polars_tf')
con.query('SELECT * FROM polars_tf()').to_a
end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

DuckDB::TableFunction.add_table_adapter(Polars::DataFrame, PolarsDataFrameBatchTableAdapter.new)
start_time3 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
con.expose_as_table(df, 'polars_tf_batch')
con.query('SELECT * FROM polars_tf_batch()').to_a
end_time3 = Process.clock_gettime(Process::CLOCK_MONOTONIC)

DuckDB::TableFunction.add_table_adapter(Polars::DataFrame, PolarsDataFrameOptimizedTableAdapter.new)
start_time4 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
con.expose_as_table(df, 'polars_tf_opt')
con.query('SELECT * FROM polars_tf_opt()').to_a
end_time4 = Process.clock_gettime(Process::CLOCK_MONOTONIC)

parquet_path = File.join(Dir.tmpdir, 'issue922_benchmark.parquet')
start_time2 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
query_via_parquet(con, df, 'polars_pq', parquet_path)
end_time2 = Process.clock_gettime(Process::CLOCK_MONOTONIC)

con.close
db.close
File.delete(parquet_path)

puts "Time taken for table function approach (1 row/call):              #{end_time - start_time} seconds"
puts "Time taken for table function approach (batch/call):              #{end_time3 - start_time3} seconds"
puts "Time taken for table function approach (batch + pre-extract):     #{end_time4 - start_time4} seconds"
puts "Time taken for parquet file   approach:                           #{end_time2 - start_time2} seconds"
# rubocop:enable Style/OneClassPerFile
