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

      _initialize(name.to_s)
    end
  end
end
