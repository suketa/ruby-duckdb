# frozen_string_literal: true

module DuckDB
  # DuckDB::AggregateFunctionSet encapsulates DuckDB's aggregate function set,
  # which allows registering multiple overloads of an aggregate function under one name.
  #
  # @note DuckDB::AggregateFunctionSet is experimental.
  class AggregateFunctionSet
    # @param name [String, Symbol] the function set name shared by all overloads
    # @raise [TypeError] if name is not a String or Symbol
    def initialize(name)
      raise TypeError, "#{name.class} is not a String or Symbol" unless name.is_a?(String) || name.is_a?(Symbol)

      @name = name.to_s
      _initialize(@name)
    end
  end
end
