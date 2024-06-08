# frozen_string_literal: true

require 'bigdecimal'

module DuckDB
  # The Result class encapsulates a execute result of DuckDB database.
  #
  # The usage is as follows:
  #
  #   require 'duckdb'
  #
  #   db = DuckDB::Database.open # database in memory
  #   con = db.connect
  #
  #   con.execute('CREATE TABLE users (id INTEGER, name VARCHAR(30))')
  #
  #   con.execute("INSERT into users VALUES(1, 'Alice')")
  #   con.execute("INSERT into users VALUES(2, 'Bob')")
  #   con.execute("INSERT into users VALUES(3, 'Cathy')")
  #
  #   result = con.execute('SELECT * from users')
  #   result.each do |row|
  #     p row
  #   end
  class Result
    include Enumerable
    TO_METHODS = if Gem::Version.new(DuckDB::LIBRARY_VERSION) == Gem::Version.new('0.10.0')
                   Hash.new(:_to_string).merge(
                     1 => :_to_boolean,
                     3 => :_to_smallint,
                     4 => :_to_integer,
                     5 => :_to_bigint,
                     10 => :_to_float,
                     11 => :_to_double,
                     16 => :_to_hugeint_internal,
                     19 => :_to_blob,
                     20 => :_to_decimal_internal
                   ).freeze
                 else
                   Hash.new(:_to_string).merge(
                     1 => :_to_boolean,
                     3 => :_to_smallint,
                     4 => :_to_integer,
                     5 => :_to_bigint,
                     10 => :_to_float,
                     11 => :_to_double,
                     16 => :_to_hugeint_internal,
                     18 => :_to_blob,
                     19 => :_to_decimal_internal
                   ).freeze
                 end

    alias column_size column_count
    alias row_size row_count

    @use_chunk_each = true

    class << self
      def new
        raise DuckDB::Error, 'DuckDB::Result cannot be instantiated directly.'
      end

      def use_chunk_each=(value)
        warn('`changing DuckDB::Result.use_chunk_each to false` will be deprecated.') if value == false

        @use_chunk_each = value
      end

      def use_chunk_each?
        !!@use_chunk_each
      end
    end

    def each
      if self.class.use_chunk_each?
        if streaming?
          return _chunk_stream unless block_given?

          _chunk_stream { |row| yield row }
        else
          return chunk_each unless block_given?

          chunk_each { |row| yield row }
        end
      else
        warn('this `each` behavior will be deprecated in the future. set `DuckDB::Result.use_chunk_each = true` to use new `each` behavior.')
        return to_enum { row_size } unless block_given?

        row_count.times do |row_index|
          yield row(row_index)
        end
      end
    end

    def row(row_index)
      warn("#{self.class}##{__method__} will be deprecated. set `DuckDB::Result.use_chunk_each = true`.")
      row = []
      column_count.times do |col_index|
        row << (_null?(row_index, col_index) ? nil : to_value(row_index, col_index))
      end
      row
    end

    def to_value(row_index, col_index)
      warn("#{self.class}##{__method__} will be deprecated. set `DuckDB::Result.use_chunk_each = true`.")
      send(TO_METHODS[_column_type(col_index)], row_index, col_index)
    end

    def enum_dictionary_values(col_index)
      values = []
      _enum_dictionary_size(col_index).times do |i|
        values << _enum_dictionary_value(col_index, i)
      end
      values
    end

    private

    def _to_hugeint(row, col)
      warn("#{self.class}##{__method__} will be deprecated.")
      _to_string(row, col).to_i
    end

    def _to_hugeint_internal(row, col)
      warn("#{self.class}##{__method__} will be deprecated.")
      lower, upper = __to_hugeint_internal(row, col)
      Converter._to_hugeint_from_vector(lower, upper)
    end

    def _to_decimal(row, col)
      warn("#{self.class}##{__method__} will be deprecated.")
      BigDecimal(_to_string(row, col))
    end

    def _to_decimal_internal(row, col)
      warn("#{self.class}##{__method__} will be deprecated.")
      lower, upper, width, scale = __to_decimal_internal(row, col)
      Converter._to_decimal_from_hugeint(width, scale, upper, lower)
    end
  end
end
