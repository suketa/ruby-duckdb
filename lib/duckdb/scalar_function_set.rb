# frozen_string_literal: true

module DuckDB
  # DuckDB::ScalarFunctionSet encapsulates DuckDB's scalar function set,
  # which allows registering multiple overloads of a scalar function under one name.
  #
  # @note DuckDB::ScalarFunctionSet is experimental.
  class ScalarFunctionSet
    # @param name [String, Symbol] the function set name shared by all overloads
    # @raise [TypeError] if name is not a String or Symbol
    def initialize(name)
      raise TypeError, "#{name.class} is not a String or Symbol" unless name.is_a?(String) || name.is_a?(Symbol)

      @name = name.to_s
      _initialize(@name)
    end

    # @param scalar_function [DuckDB::ScalarFunction] the overload to add
    # @return [self]
    # @raise [TypeError] if scalar_function is not a DuckDB::ScalarFunction
    # @raise [DuckDB::Error] if the overload already exists in the set
    def add(scalar_function)
      unless scalar_function.is_a?(DuckDB::ScalarFunction)
        raise TypeError, "#{scalar_function.class} is not a DuckDB::ScalarFunction"
      end

      scalar_function.name = @name
      _add(scalar_function)
    end
  end
end
