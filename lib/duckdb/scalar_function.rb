# frozen_string_literal: true

module DuckDB
  # DuckDB::ScalarFunction encapsulates DuckDB's scalar function
  class ScalarFunction
    # Sets the return type for the scalar function.
    # Currently only INTEGER type is supported.
    #
    # @param logical_type [DuckDB::LogicalType] the return type
    # @return [DuckDB::ScalarFunction] self
    # @raise [DuckDB::Error] if the type is not INTEGER
    def return_type=(logical_type)
      unless logical_type.is_a?(DuckDB::LogicalType)
        raise DuckDB::Error, 'logical_type must be a DuckDB::LogicalType'
      end

      # Check if the type is INTEGER
      unless logical_type.type == :integer
        raise DuckDB::Error, 'Only INTEGER return type is currently supported'
      end

      _set_return_type(logical_type)
      self
    end
  end
end
