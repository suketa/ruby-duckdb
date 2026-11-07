# frozen_string_literal: true

module DuckDB
  #
  # The DuckDB::DataChunk represents a chunk of data for table function output.
  #
  # During table function execution, data chunks are used to return rows.
  #
  # Example:
  #
  #   done = false
  #   table_function.init { |_init_info| done = false }
  #
  #   table_function.execute do |func_info, output|
  #     if done
  #       output.size = 0  # Signal completion
  #     else
  #       # High-level API
  #       output.set_value(0, 0, 42)        # column 0, row 0, value 42
  #       output.set_value(1, 0, 'Alice')   # column 1, row 0, value 'Alice'
  #       output.size = 1
  #       done = true
  #     end
  #   end
  #
  class DataChunk
    # Most methods are defined in C extension (ext/duckdb/data_chunk.c).
    #
    # DataChunk#set_value(col_idx, row_idx, value) converts +value+ by the
    # column's logical type with the same writer ScalarFunction uses for its
    # return values, so both accept the same types with identical results.

    #
    # Resets the data chunk so it can be reused for another batch of rows.
    #
    # @return [DuckDB::DataChunk] self
    #
    def reset
      _reset
      self
    end
  end
end
