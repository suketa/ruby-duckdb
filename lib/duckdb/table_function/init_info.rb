# frozen_string_literal: true

module DuckDB
  class TableFunction
    #
    # The DuckDB::TableFunction::InitInfo provides context during table function initialization.
    #
    # It is passed to the init callback to set up execution state.
    #
    # Example:
    #
    #   table_function.init do |init_info|
    #     # Initialize execution state
    #     # Can report errors if initialization fails
    #     init_info.set_error('Initialization failed')
    #   end
    #
    # rubocop:disable Lint/EmptyClass
    class InitInfo
      # All methods are defined in C extension (ext/duckdb/table_function_init_info.c)
    end
    # rubocop:enable Lint/EmptyClass
  end
end
