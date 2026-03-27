# frozen_string_literal: true

module DuckDB
  class ScalarFunction
    # DuckDB::ScalarFunction::BindInfo encapsulates the bind phase of a scalar function.
    #
    # An instance is passed to the block given to ScalarFunction#set_bind.
    # The bind phase runs once at query planning time (before execution).
    #
    # Example:
    #
    #   sf.set_bind do |bind_info|
    #     if bind_info.argument_count != 1
    #       bind_info.set_error('expected exactly 1 argument')
    #     end
    #   end
    #
    class BindInfo
      # Returns the DuckDB::Expression for the argument at +index+.
      # Raises ArgumentError if +index+ is out of range.
      #
      #   sf.set_bind do |bind_info|
      #     expr = bind_info.get_argument(0)
      #   end
      def get_argument(index)
        if index.negative? || index >= argument_count
          raise ArgumentError,
                "index #{index} is out of range (argument_count: #{argument_count})"
        end

        _get_argument(index)
      end
    end
  end
end
