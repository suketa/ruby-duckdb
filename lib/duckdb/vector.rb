# frozen_string_literal: true

module DuckDB
  #
  # The DuckDB::Vector represents a column vector in a data chunk.
  #
  # Vectors store the actual data for table function output.
  #
  # Example:
  #
  #   vector = output.get_vector(0)
  #   vector.assign_string_element(0, 'hello')
  #   vector.assign_string_element(1, 'world')
  #
  # rubocop:disable Lint/EmptyClass
  class Vector
    # All methods are defined in C extension (ext/duckdb/vector.c)
  end
  # rubocop:enable Lint/EmptyClass
end
