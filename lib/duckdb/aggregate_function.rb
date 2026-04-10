# frozen_string_literal: true

module DuckDB
  # DuckDB::AggregateFunction encapsulates DuckDB's aggregate function.
  #
  # @note DuckDB::AggregateFunction is experimental. Phase 1.0 only supports
  #   +set_init+ and +set_finalize+; +update+ and +combine+ are internal no-ops.
  class AggregateFunction
    # Supported types for aggregate function parameters and return values
    SUPPORTED_TYPES = %i[
      any
      bigint
      blob
      boolean
      date
      decimal
      double
      float
      hugeint
      integer
      interval
      smallint
      time
      timestamp
      timestamp_s
      timestamp_ms
      timestamp_ns
      time_tz
      timestamp_tz
      tinyint
      ubigint
      uhugeint
      uinteger
      usmallint
      utinyint
      uuid
      varchar
    ].freeze

    private_constant :SUPPORTED_TYPES

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

    private

    def check_supported_type!(type)
      logical_type = DuckDB::LogicalType.resolve(type)

      unless SUPPORTED_TYPES.include?(logical_type.type)
        raise DuckDB::Error, "Type `#{type}` is not supported. Only #{SUPPORTED_TYPES.inspect} are available."
      end

      logical_type
    end
  end
end
