# frozen_string_literal: true

require 'duckdb'
require 'polars-df'

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

DuckDB::TableFunction.add_table_adapter(Polars::DataFrame, PolarsDataFrameTableAdapter.new)

df = polars.dataframe.new(
  {
    a: [1, 2, 3],
    b: %w[one two three]
  }
)

db = DuckDB::Database.open
con = db.connect
con.query('SET threads=1')
con.expose_as_table(df, 'polars_df')
result = con.query('SELECT * FROM polars_df()').to_a
p result
puts result.first.first == '1'

con.close
db.close
