# frozen_string_literal: true

module DuckDB
  # DuckDB::ScalarFunction encapsulates DuckDB's scalar function
  #
  # @note DuckDB::ScalarFunction is experimental.
  # @note DuckDB::ScalarFunction must be used with threads=1 in DuckDB.
  #       Set this with: connection.execute('SET threads=1')
  class ScalarFunction
    # Create and configure a scalar function in one call
    #
    # @param name [String, Symbol] the function name
    # @param return_type [DuckDB::LogicalType] the return type
    # @param parameter_type [DuckDB::LogicalType, nil] single parameter type (use this OR parameter_types)
    # @param parameter_types [Array<DuckDB::LogicalType>, nil] multiple parameter types
    # @yield [*args] the function implementation
    # @return [DuckDB::ScalarFunction] configured scalar function ready to register
    # @raise [ArgumentError] if block is not provided or both parameter_type and parameter_types are specified
    #
    # @example Single parameter function
    #   sf = DuckDB::ScalarFunction.create(
    #     name: :triple,
    #     return_type: DuckDB::LogicalType::INTEGER,
    #     parameter_type: DuckDB::LogicalType::INTEGER
    #   ) { |v| v * 3 }
    #
    # @example Multiple parameters
    #   sf = DuckDB::ScalarFunction.create(
    #     name: :add,
    #     return_type: DuckDB::LogicalType::INTEGER,
    #     parameter_types: [DuckDB::LogicalType::INTEGER, DuckDB::LogicalType::INTEGER]
    #   ) { |a, b| a + b }
    #
    # @example No parameters (constant function)
    #   sf = DuckDB::ScalarFunction.create(
    #     name: :random_num,
    #     return_type: DuckDB::LogicalType::INTEGER
    #   ) { rand(100) }
    def self.create(name:, return_type:, parameter_type: nil, parameter_types: nil, &) # rubocop:disable Metrics/MethodLength
      raise ArgumentError, 'Block required' unless block_given?
      raise ArgumentError, 'Cannot specify both parameter_type and parameter_types' if parameter_type && parameter_types

      params = if parameter_type
                 [parameter_type]
               elsif parameter_types
                 parameter_types
               else
                 []
               end

      sf = new
      sf.name = name.to_s
      sf.return_type = return_type
      params.each { |type| sf.add_parameter(type) }
      sf.set_function(&)
      sf
    end

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
