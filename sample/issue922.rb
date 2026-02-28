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
  class TableFunction
    @table_adapters = {}
    class << self
      def add_table_adapter(klass, adapter)
        @table_adapters[klass] = adapter
      end

      def table_adapter_for(klass)
        @table_adapters[klass]
      end
    end
  end

  class Connection
    def create_table_function(object, name)
      adapter = TableFunction.table_adapter_for(object.class)
      raise ArgumentError, "No table adapter registered for #{object.class}" if adapter.nil?

      tf = adapter.call(object, name)
      register_table_function(tf)
    end
  end

  module Polars
    module DataFrame
      class TableAdapter
        def call(df, name) # rubocop:disable Metrics/MethodLength, Naming/MethodParameterName
          columns = df.columns.each_with_object({}) { |header, hash| hash[header] = LogicalType::VARCHAR }
          counter = 0
          height = df.height
          width = df.columns.length

          DuckDB::TableFunction.create(
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
              0
            end
          end
        end
      end
    end
  end
  TableFunction.add_table_adapter(::Polars::DataFrame, DuckDB::Polars::DataFrame::TableAdapter.new)
end

db = DuckDB::Database.open

con = db.connect
con.query('SET threads=1')
con.create_table_function(df, 'polars_df')
result = con.query('SELECT * FROM polars_df()').to_a
p result
puts result.first.first == '1'

con.close
db.close
