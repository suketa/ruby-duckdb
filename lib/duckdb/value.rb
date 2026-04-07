# frozen_string_literal: true

module DuckDB
  class Value
    class << self
      def create_bool(value)
        raise ArgumentError, 'expected true or false' unless value.is_a?(TrueClass) || value.is_a?(FalseClass)

        _create_bool(value)
      end
    end
  end
end
