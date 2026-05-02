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

    class << self
      def new
        raise DuckDB::Error, 'DuckDB::Result cannot be instantiated directly.'
      end
    end

    def each(&)
      return _chunk_stream unless block_given?

      _chunk_stream(&)
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
      column = columns[col_index]

      raise ArgumentError, "Invalid index: #{col_index}" if column.nil? || col_index.negative?

      lt = column.logical_type

      raise DuckDB::Error, "Column[#{col_index}] type is not enum" if lt.type != :enum

      values = []
      lt.dictionary_size.times do |i|
        values << lt.dictionary_value_at(i)
      end
      values
    end

    private

    def _enum_dictionary_size(idx)
      warn(":_enum_dictionary_size is deprecated. use columns[#{idx}].logical_type.dictionary_size instead.")

      raise ArgumentError, "Invalid index: #{idx}" if idx.negative?

      columns[idx]&.logical_type&.dictionary_size
    end

    def _enum_dictionary_value(col_index, idx)
      warn(":_enum_dictionary_value is deprecated.\
           use columns[#{col_index}].logical_type.dictionary_value_at(#{idx}) instead.")

      raise ArgumentError, "Invalid index: #{col_index}" if col_index.negative?

      lt = columns[col_index]&.logical_type

      raise DuckDB::Error, "Column[#{col_index}] type is not enum" if lt.type != :enum

      lt.dictionary_value_at(idx)
    end

    def _column_type(idx)
      warn(":_column_type is deprecated. use columns[#{idx}].send(:_type) instead.")

      columns[idx].send(:_type)
    end
  end
end
