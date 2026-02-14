# frozen_string_literal: true

module DuckDB
  #
  # The DuckDB::TableFunction encapsulates a DuckDB table function.
  #
  #   require 'duckdb'
  #
  #   db = DuckDB::Database.new
  #   conn = db.connect
  #
  #   DuckDB::TableFunction.create do |tf|
  #     tf.name = 'my_function'
  #     tf.add_parameter(DuckDB::LogicalType::BIGINT)
  #
  #     tf.bind do |bind_info|
  #       bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
  #     end
  #
  #     tf.execute do |func_info, output|
  #       # Fill output data...
  #       output.size = 0
  #     end
  #
  #     conn.register_table_function(tf)
  #   end
  #
  class TableFunction
    # TableFunction.create is defined in C extension
  end
end
