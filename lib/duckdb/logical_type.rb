# frozen_string_literal: true

module DuckDB
  class LogicalType
    # returns logical type's type symbol
    # `:unknown` means that the logical type's type is unknown/unsupported by ruby-duckdb.
    # `:invalid` means that the logical type's type is invalid in duckdb.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE climates (id INTEGER, temperature DECIMAIL)')
    #
    #   users = con.query('SELECT * FROM climates')
    #   columns = users.columns
    #   columns.second.logical_type.type #=> :decimal
    def type
      type_id = _type
      DuckDB::Converter::IntToSym.type_to_sym(type_id)
    end

    # Iterates over each union member name.
    #
    # When a block is provided, this method yields each union member name in
    # order. It also returns the total number of members yielded.
    #
    #   union_logical_type.each_member_name do |name|
    #     puts "Union member: #{name}"
    #   end
    #
    # If no block is given, an Enumerator is returned, which can be used to
    # retrieve all member names.
    #
    #   names = union_logical_type.each_member_name.to_a
    #   # => ["member1", "member2"]
    def each_member_name
      return to_enum(__method__) {member_count} unless block_given?

      member_count.times do |i|
        yield member_name_at(i)
      end
    end
  end
end
