# frozen_string_literal: true

module DuckDB
  #
  # The DuckDB::DataChunk represents a chunk of data for table function output.
  #
  # During table function execution, data chunks are used to return rows.
  #
  # Example:
  #
  #   table_function.execute do |func_info, output|
  #     # Set number of rows to output
  #     output.size = 10
  #
  #     # Get vector for column 0
  #     vector = output.get_vector(0)
  #
  #     # Write data to vector...
  #   end
  #
  # rubocop:disable Lint/EmptyClass
  class DataChunk
    # All methods are defined in C extension (ext/duckdb/data_chunk.c)
  end
  # rubocop:enable Lint/EmptyClass
end
