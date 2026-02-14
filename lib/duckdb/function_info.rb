# frozen_string_literal: true

module DuckDB
  #
  # The DuckDB::FunctionInfo provides context during table function execution.
  #
  # It is passed to the execute callback along with the output DataChunk.
  #
  # Example:
  #
  #   table_function.execute do |func_info, output|
  #     # Report errors during execution
  #     func_info.set_error('Something went wrong')
  #   end
  #
  # rubocop:disable Lint/EmptyClass
  class FunctionInfo
    # All methods are defined in C extension (ext/duckdb/function_info.c)
  end
  # rubocop:enable Lint/EmptyClass
end
