# frozen_string_literal: true

module DuckDB
  # DuckDB::Expression represents a DuckDB expression object.
  #
  # Instances are returned by DuckDB::ScalarFunction::BindInfo#get_argument
  # during the bind phase of a scalar function.
  class Expression
    # Evaluates the expression at planning time and returns a DuckDB::Value
    # holding the constant result.
    # Raises DuckDB::Error if the expression is not foldable.
    #
    #   sf.set_bind do |bind_info|
    #     expr           = bind_info.get_argument(0)
    #     client_context = bind_info.client_context
    #     value          = expr.fold(client_context)   # => DuckDB::Value
    #   end
    def fold(client_context)
      raise DuckDB::Error, 'expression is not foldable' unless foldable?

      _fold(client_context)
    end
  end
end
