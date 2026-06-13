# frozen_string_literal: true

module DuckDB
  # The ArrowArrayStream class represents an exported Arrow C stream of a
  # query result (Arrow C Data Interface). It is created by
  # DuckDB::Result#arrow_c_stream and cannot be instantiated directly.
  #
  # The object satisfies the Ruby Arrow C stream protocol: #arrow_c_stream
  # returns self and #to_i returns the address of the underlying
  # <tt>struct ArrowArrayStream</tt>, so it can be consumed by ruby-polars,
  # red-arrow and other Arrow consumers:
  #
  #   result = con.query('SELECT * FROM users')
  #
  #   # ruby-polars
  #   df = Polars::DataFrame.new(result)
  #
  #   # red-arrow
  #   reader = Arrow::RecordBatchReader.import(result.arrow_c_stream.to_i)
  #
  # The consumer takes ownership of the stream's contents; a result can be
  # exported only once.
  #
  # [EXPERIMENTAL] This API is built on DuckDB's unstable Arrow C API and
  # may change in any minor release.
  class ArrowArrayStream
    class << self
      def new
        raise DuckDB::Error, 'DuckDB::ArrowArrayStream cannot be instantiated directly. Use DuckDB::Result#arrow_c_stream.'
      end
    end
  end
end
