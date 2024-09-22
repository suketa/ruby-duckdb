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
    RETURN_TYPES = %i[invalid changed_rows nothing query_result].freeze

    alias column_size column_count
    alias row_size row_count

    class << self
      def new
        raise DuckDB::Error, 'DuckDB::Result cannot be instantiated directly.'
      end

      def use_chunk_each=(value) # :nodoc:
        raise('`changing DuckDB::Result.use_chunk_each to false` was deprecated.') unless value

        warn('`DuckDB::Result.use_chunk_each=` will be deprecated.')

        true
      end

      def use_chunk_each? # :nodoc:
        warn('`DuckDB::Result.use_chunk_each?` will be deprecated.')
        true
      end
    end

    def each(&)
      if streaming?
        return _chunk_stream unless block_given?

        _chunk_stream(&)
      else
        return chunk_each unless block_given?

        chunk_each(&)
      end
    end

    # returns return type. The return value is one of the following symbols:
    #  :invalid, :changed_rows, :nothing, :query_result
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_database')
    #   con = db.connect
    #   result = con.execute('CREATE TABLE users (id INTEGER, name VARCHAR(30))')
    #   result.return_type # => :nothing
    def return_type
      i = _return_type
      raise DuckDB::Error, "Unknown return type: #{i}" if i >= RETURN_TYPES.size

      RETURN_TYPES[i]
    end

    # returns statement type. The return value is one of the following symbols:
    #  :invalid, :select, :insert, :update, :explain, :delete, :prepare, :create,
    #  :execute, :alter, :transaction, :copy, :analyze, :variable_set, :create_func,
    #  :drop, :export, :pragma, :vacuum, :call, :set, :load, :relation, :extension,
    #  :logical_plan, :attach, :detach, :multi
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_database')
    #   con = db.connect
    #   result = con.execute('CREATE TABLE users (id INTEGER, name VARCHAR(30))')
    #   result.statement_type # => :create
    def statement_type
      i = _statement_type
      Converter::IntToSym.statement_type_to_sym(i)
    end

    # returns all available ENUM type values of the specified column index.
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_database')
    #   con = db.connect
    #   con.execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy', '𝘾𝝾օɭ 😎')")
    #   con.execute("CREATE TABLE enums (id INTEGER, mood mood)")
    #   result = con.query('SELECT * FROM enums')
    #   result.enum_dictionary_values(1) # => ['sad', 'ok', 'happy', '𝘾𝝾օɭ 😎']
    def enum_dictionary_values(col_index)
      values = []
      _enum_dictionary_size(col_index).times do |i|
        values << _enum_dictionary_value(col_index, i)
      end
      values
    end
  end
end
