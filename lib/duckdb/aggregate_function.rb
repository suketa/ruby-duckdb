# frozen_string_literal: true

module DuckDB
  # DuckDB::AggregateFunction encapsulates DuckDB's aggregate function.
  #
  # @note DuckDB::AggregateFunction is experimental. Phase 1.0 only supports
  #   +set_init+ and +set_finalize+; +update+ and +combine+ are internal no-ops.
  class AggregateFunction
    # Sets the return type for the aggregate function.
    #
    # @param logical_type [DuckDB::LogicalType] the return type
    # @return [DuckDB::AggregateFunction] self
    def return_type=(logical_type)
      _set_return_type(logical_type)
    end

    # Adds a parameter to the aggregate function.
    #
    # @param logical_type [DuckDB::LogicalType] the parameter type
    # @return [DuckDB::AggregateFunction] self
    def add_parameter(logical_type)
      _add_parameter(logical_type)
    end
  end
end
