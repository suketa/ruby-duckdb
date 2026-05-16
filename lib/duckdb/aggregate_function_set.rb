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

    # @param aggregate_function [DuckDB::AggregateFunction] the overload to add
    # @return [self]
    # @raise [TypeError] if aggregate_function is not a DuckDB::AggregateFunction
    # @raise [DuckDB::Error] if the overload already exists in the set
    def add(aggregate_function)
      unless aggregate_function.is_a?(DuckDB::AggregateFunction)
        raise TypeError, "#{aggregate_function.class} is not a DuckDB::AggregateFunction"
      end

      _add(aggregate_function)
    end
  end
end
