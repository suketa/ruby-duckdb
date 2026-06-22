# frozen_string_literal: true

module DuckDB
  # The exception raised by ruby-duckdb.
  class Error < StandardError
    # +error_type_id+ is the raw DuckDB error type id, set by the C extension when
    # a query result fails; it defaults to +nil+ for all other errors.
    def initialize(message = nil, error_type_id = nil)
      super(message)
      @error_type_id = error_type_id
    end

    # Returns the DuckDB error category as a Symbol (e.g. +:constraint+,
    # +:catalog+, +:parser+), or +nil+ when the error did not originate from a
    # DuckDB query result (e.g. internal binding failures).
    #
    #   begin
    #     con.query('INSERT INTO t VALUES (1)') # duplicate primary key
    #   rescue DuckDB::Error => e
    #     e.error_type # => :constraint
    #   end
    def error_type
      @error_type_id && Converter::IntToSym.error_type_to_sym(@error_type_id)
    end
  end
end
