# frozen_string_literal: true

module DuckDB
  class LogicalType
    alias :alias get_alias
    alias :alias= set_alias

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

    # returns logical type's internal type symbol for Decimal or Enum types
    # `:unknown` means that the logical type's type is unknown/unsupported by ruby-duckdb.
    # `:invalid` means that the logical type's type is invalid in duckdb.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query("CREATE TYPE mood AS ENUM ('happy', 'sad')")
    #   con.query("CREATE TABLE emotions (id INTEGER, enum_col mood)")
    #
    #   users = con.query('SELECT * FROM emotions')
    #   ernum_col = users.columns.find { |col| col.name == 'enum_col' }
    #   enum_col.logical_type.internal_type #=> :utinyint
    def internal_type
      type_id = _internal_type
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

    # Iterates over each union member type.
    #
    # When a block is provided, this method yields each union member logical
    # type in order. It also returns the total number of members yielded.
    #
    #   union_logical_type.each_member_type do |logical_type|
    #     puts "Union member: #{logical_type.type}"
    #   end
    #
    # If no block is given, an Enumerator is returned, which can be used to
    # retrieve all member logical types.
    #
    #   names = union_logical_type.each_member_type.map(&:type)
    #   # => [:varchar, :integer]
    def each_member_type
      return to_enum(__method__) {member_count} unless block_given?

      member_count.times do |i|
        yield member_type_at(i)
      end
    end

    # Iterates over each struct child name.
    #
    # When a block is provided, this method yields each struct child name in
    # order. It also returns the total number of children yielded.
    #
    #   struct_logical_type.each_child_name do |name|
    #     puts "Struct child: #{name}"
    #   end
    #
    # If no block is given, an Enumerator is returned, which can be used to
    # retrieve all child names.
    #
    #   names = struct_logical_type.each_child_name.to_a
    #   # => ["child1", "child2"]
    def each_child_name
      return to_enum(__method__) {child_count} unless block_given?

      child_count.times do |i|
        yield child_name_at(i)
      end
    end

    # Iterates over each struct child type.
    #
    # When a block is provided, this method yields each struct child type in
    # order. It also returns the total number of children yielded.
    #
    #   struct_logical_type.each_child_type do |logical_type|
    #     puts "Struct child type: #{logical_type.type}"
    #   end
    #
    # If no block is given, an Enumerator is returned, which can be used to
    # retrieve all child logical types.
    #
    #   types = struct_logical_type.each_child_type.map(&:type)
    #   # => [:integer, :varchar]
    def each_child_type
      return to_enum(__method__) {child_count} unless block_given?

      child_count.times do |i|
        yield child_type_at(i)
      end
    end

    # Iterates over each enum dictionary value.
    #
    # When a block is provided, this method yields each enum dictionary value
    # in order. It also returns the total number of dictionary values yielded.
    #
    #   enum_logical_type.each_value do |value|
    #     puts "Enum value: #{value}"
    #   end
    #
    # If no block is given, an Enumerator is returned, which can be used to
    # retrieve all enum dictionary values.
    #
    #   values = enum_logical_type.each_value.to_a
    #   # => ["happy", "sad"]
    def each_dictionary_value
      return to_enum(__method__) {dictionary_size} unless block_given?

      dictionary_size.times do |i|
        yield dictionary_value_at(i)
      end
    end
  end
end
