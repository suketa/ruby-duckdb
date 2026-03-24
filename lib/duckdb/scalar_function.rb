# frozen_string_literal: true

module DuckDB
  # DuckDB::ScalarFunction encapsulates DuckDB's scalar function
  #
  # @note DuckDB::ScalarFunction is experimental.
  class ScalarFunction
    # Create and configure a scalar function in one call
    #
    # @param name [String, Symbol] the function name
    # @param return_type [DuckDB::LogicalType|:logical_type_symbol] the return type
    # @param parameter_type [DuckDB::LogicalType|:logical_type_symbol, nil] single fixed parameter type
    # @param parameter_types [Array<DuckDB::LogicalType|:logical_type_symbol>, nil] multiple fixed parameter types
    # @param varargs_type [DuckDB::LogicalType|:logical_type_symbol, nil] trailing varargs element type;
    #   can be combined with parameter_type/parameter_types to add fixed leading parameters
    # @yield [fixed_args..., *varargs] the function implementation
    # @return [DuckDB::ScalarFunction] configured scalar function ready to register
    # @raise [ArgumentError] if block is not provided, or both parameter_type and parameter_types are specified
    #
    # @example Single parameter function
    #   sf = DuckDB::ScalarFunction.create(
    #     name: :triple,
    #     return_type: :integer,
    #     parameter_type: :integer
    #   ) { |v| v * 3 }
    #
    # @example Multiple parameters
    #   sf = DuckDB::ScalarFunction.create(
    #     name: :add,
    #     return_type: :integer,
    #     parameter_types: [:integer, :integer]
    #   ) { |a, b| a + b }
    #
    # @example Varargs function (variable number of arguments)
    #   sf = DuckDB::ScalarFunction.create(
    #     name: :sum_all,
    #     return_type: :integer,
    #     varargs_type: :integer
    #   ) { |*args| args.sum }
    #
    # @example Fixed parameter(s) followed by varargs
    #   sf = DuckDB::ScalarFunction.create(
    #     name: :join_with_sep,
    #     return_type: :varchar,
    #     parameter_type: :varchar,  # separator
    #     varargs_type: :varchar     # values to join
    #   ) { |sep, *parts| parts.join(sep) }
    #
    # @example No parameters (constant function)
    #   sf = DuckDB::ScalarFunction.create(
    #     name: :random_num,
    #     return_type: :integer
    #   ) { rand(100) }
    def self.create( # rubocop:disable Metrics/MethodLength,Metrics/CyclomaticComplexity,Metrics/PerceivedComplexity
      name:, return_type:, parameter_type: nil, parameter_types: nil, varargs_type: nil, &
    )
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
      sf.varargs_type = varargs_type if varargs_type
      sf.set_function(&)
      sf
    end

    # Supported types for scalar function parameters and return values
    SUPPORTED_TYPES = %i[
      any
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
    # @param logical_type [DuckDB::LogicalType | :logical_type_symbol] the parameter type
    # @return [DuckDB::ScalarFunction] self
    # @raise [DuckDB::Error] if the type is not supported
    def add_parameter(logical_type)
      logical_type = check_supported_type!(logical_type)

      _add_parameter(logical_type)
    end

    # Sets the return type for the scalar function.
    # Currently supports BIGINT, BLOB, BOOLEAN, DATE, DOUBLE, FLOAT, INTEGER, SMALLINT, TIME, TIMESTAMP, TINYINT,
    # UBIGINT, UINTEGER, USMALLINT, UTINYINT, and VARCHAR types.
    #
    # @param logical_type [DuckDB::LogicalType | :logical_type_symbol] the return type
    # @return [DuckDB::ScalarFunction] self
    # @raise [DuckDB::Error] if the type is not supported
    def return_type=(logical_type)
      logical_type = check_supported_type!(logical_type)

      _set_return_type(logical_type)
    end

    # Sets special NULL handling for the scalar function.
    # By default DuckDB skips the callback and returns NULL when any input row
    # contains a NULL argument. Calling this method disables that behaviour so
    # the block is invoked even when inputs are NULL, receiving +nil+ for each
    # NULL argument. This lets the function implement its own NULL semantics
    # (e.g. treating NULL as 0, or counting NULLs).
    #
    # Wraps +duckdb_scalar_function_set_special_handling+.
    #
    # @return [DuckDB::ScalarFunction] self
    def set_special_handling
      _set_special_handling
    end

    # Sets the varargs type for the scalar function.
    # Marks the function to accept a variable number of trailing arguments all of the
    # given type. Can be combined with add_parameter to add fixed leading parameters
    # (e.g. a separator followed by a variable list of values).
    # The block receives fixed parameters positionally, then varargs as a splat (|fixed, *rest|).
    # Currently supports BIGINT, BLOB, BOOLEAN, DATE, DOUBLE, FLOAT, INTEGER, SMALLINT, TIME, TIMESTAMP, TINYINT,
    # UBIGINT, UINTEGER, USMALLINT, UTINYINT, and VARCHAR types.
    #
    # @param logical_type [DuckDB::LogicalType | :logical_type_symbol] the varargs element type
    # @return [DuckDB::ScalarFunction] self
    # @raise [DuckDB::Error] if the type is not supported
    def varargs_type=(logical_type)
      logical_type = check_supported_type!(logical_type)

      _set_varargs(logical_type)
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
