# frozen_string_literal: true

module DuckDB
  # DuckDB::AggregateFunction encapsulates DuckDB's aggregate function.
  #
  # @note DuckDB::AggregateFunction is experimental. Phase 1.0 only supports
  #   +set_init+ and +set_finalize+; +update+ and +combine+ are internal no-ops.
  class AggregateFunction
    include FunctionTypeValidation

    # Sets the return type for the aggregate function.
    #
    # @param logical_type [DuckDB::LogicalType | :logical_type_symbol] the return type
    # @return [DuckDB::AggregateFunction] self
    # @raise [DuckDB::Error] if the type is not supported
    def return_type=(logical_type)
      logical_type = check_supported_type!(logical_type)

      _set_return_type(logical_type)
    end

    # Adds a parameter to the aggregate function.
    #
    # @param logical_type [DuckDB::LogicalType | :logical_type_symbol] the parameter type
    # @return [DuckDB::AggregateFunction] self
    # @raise [DuckDB::Error] if the type is not supported
    def add_parameter(logical_type)
      logical_type = check_supported_type!(logical_type)

      _add_parameter(logical_type)
    end

    def set_init(&block)
      _set_combine { |s1, _s2| s1 } unless @combine_set
      _set_finalize { |x| x } unless @finalize_set
      _set_init(&block)
    end

    def set_update(&block)
      _set_update(&block)
    end

    def set_combine(&block)
      @combine_set = true
      _set_combine(&block)
    end

    def set_finalize(&block)
      @finalize_set = true
      _set_finalize(&block)
    end

    # Sets special NULL handling for the aggregate function.
    # By default DuckDB skips rows with NULL input values.  Calling this
    # method disables that behaviour so the update callback is invoked even
    # when inputs are NULL, receiving +nil+ for each NULL argument.  This
    # lets the function implement its own NULL semantics (e.g. counting
    # NULLs).
    #
    # Wraps +duckdb_aggregate_function_set_special_handling+.
    #
    # @return [DuckDB::AggregateFunction] self
    def set_special_handling
      _set_special_handling
    end
  end
end
