# frozen_string_literal: true

module DuckDB
  class TableFunction
    #
    # The DuckDB::TableFunction::BindInfo encapsulates information for the bind phase of table functions.
    #
    # During the bind phase, you can:
    # - Access parameters passed to the function
    # - Define the output schema (columns)
    # - Set performance hints (cardinality)
    # - Report errors
    #
    # Example:
    #
    #   table_function.bind do |bind_info|
    #     # Get parameters
    #     limit = bind_info.get_parameter(0).to_i
    #
    #     # Define output schema
    #     bind_info.add_result_column('id', DuckDB::LogicalType::BIGINT)
    #     bind_info.add_result_column('name', DuckDB::LogicalType::VARCHAR)
    #
    #     # Set cardinality hint
    #     bind_info.set_cardinality(limit, true)
    #   end
    #
    class BindInfo
      def add_result_column(name, type)
        _add_result_column(name.to_s, DuckDB::LogicalType.resolve(type))
      end
    end
  end
end
