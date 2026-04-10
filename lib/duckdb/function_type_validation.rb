# frozen_string_literal: true

module DuckDB
  # Shared type-checking mixin for DuckDB function classes.
  # Provides +SUPPORTED_TYPES+ and the private +check_supported_type!+ guard
  # used by +ScalarFunction+ and +AggregateFunction+.
  module FunctionTypeValidation
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
