# frozen_string_literal: true

require 'duckdb'
require 'polars-df'

df = Polars::DataFrame.new(
  {
    a: [1, 2, 3],
    b: %w[one two three]
  }
)

module DuckDB
  class Connection
    def register_as_table(name, df) # rubocop:disable Metrics/MethodLength, Naming/MethodParameterName
      columns = df.columns.each_with_object({}) { |header, hash| hash[header] = LogicalType::VARCHAR }
      counter = 0
      height = df.height
      width = df.columns.length
      tf = DuckDB::TableFunction.create(
        name:,
        columns:
      ) do |_func_info, output|
        if counter < height
          width.times do |index|
            output.set_value(index, 0, df[counter, index])
          end
          counter += 1
          1
        else
          counter = 0
        end
      end
      register_table_function(tf)
    end
  end
end

db = DuckDB::Database.open

con = db.connect
con.query('SET threads=1')
con.register_as_table('polars_df', df)
result = con.query('SELECT * FROM polars_df()').to_a
p result
puts result.first.first == '1'

con.close
db.close
