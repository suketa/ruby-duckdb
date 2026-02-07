# frozen_string_literal: true

module DuckDB
  # DuckDB::ScalarFunction encapsulates DuckDB's scalar function
  class ScalarFunction
    # Sets the return type for the scalar function.
    # Currently supports BOOLEAN, INTEGER, BIGINT, FLOAT, and DOUBLE types.
    #
    # @param logical_type [DuckDB::LogicalType] the return type
    # @return [DuckDB::ScalarFunction] self
    # @raise [DuckDB::Error] if the type is not supported
    def return_type=(logical_type)
      raise DuckDB::Error, 'logical_type must be a DuckDB::LogicalType' unless logical_type.is_a?(DuckDB::LogicalType)

      # Check if the type is supported
      unless %i[boolean integer bigint float double].include?(logical_type.type)
        raise DuckDB::Error, 'Only BOOLEAN, INTEGER, BIGINT, FLOAT, and DOUBLE return types are currently supported'
      end

      _set_return_type(logical_type)
    end
  end
end
