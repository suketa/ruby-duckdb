# frozen_string_literal: true

module DuckDB
  # DuckDB::ScalarFunction encapsulates DuckDB's scalar function
  #
  # @note DuckDB::ScalarFunction is experimental.
  # @note DuckDB::ScalarFunction must be used with threads=1 in DuckDB.
  #       Set this with: connection.execute('SET threads=1')
  class ScalarFunction
    # Supported types for scalar function parameters and return values
    SUPPORTED_TYPES = %i[
      bigint
      blob
      boolean
      date
      double
      float
      integer
      smallint
      time
      timestamp
      tinyint
      ubigint
      uinteger
      usmallint
      utinyint
      varchar
    ].freeze

    private_constant :SUPPORTED_TYPES

    # Adds a parameter to the scalar function.
    # Currently supports BIGINT, BLOB, BOOLEAN, DATE, DOUBLE, FLOAT, INTEGER, SMALLINT, TIME, TIMESTAMP, TINYINT,
    # UBIGINT, UINTEGER, USMALLINT, UTINYINT, and VARCHAR types.
    #
    # @param logical_type [DuckDB::LogicalType] the parameter type
    # @return [DuckDB::ScalarFunction] self
    # @raise [DuckDB::Error] if the type is not supported
    def add_parameter(logical_type)
      raise DuckDB::Error, 'logical_type must be a DuckDB::LogicalType' unless logical_type.is_a?(DuckDB::LogicalType)

      unless SUPPORTED_TYPES.include?(logical_type.type)
        type_list = SUPPORTED_TYPES.map(&:upcase).join(', ')
        raise DuckDB::Error,
              "Only #{type_list} parameter types are currently supported"
      end

      _add_parameter(logical_type)
    end

    # Sets the return type for the scalar function.
    # Currently supports BIGINT, BLOB, BOOLEAN, DATE, DOUBLE, FLOAT, INTEGER, SMALLINT, TIME, TIMESTAMP, TINYINT,
    # UBIGINT, UINTEGER, USMALLINT, UTINYINT, and VARCHAR types.
    #
    # @param logical_type [DuckDB::LogicalType] the return type
    # @return [DuckDB::ScalarFunction] self
    # @raise [DuckDB::Error] if the type is not supported
    def return_type=(logical_type)
      raise DuckDB::Error, 'logical_type must be a DuckDB::LogicalType' unless logical_type.is_a?(DuckDB::LogicalType)

      unless SUPPORTED_TYPES.include?(logical_type.type)
        type_list = SUPPORTED_TYPES.map(&:upcase).join(', ')
        raise DuckDB::Error,
              "Only #{type_list} return types are currently supported"
      end

      _set_return_type(logical_type)
    end
  end
end
