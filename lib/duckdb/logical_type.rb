# frozen_string_literal: true

module DuckDB
  class LogicalType # rubocop:disable Metrics/ClassLength
    alias :alias get_alias
    alias :alias= set_alias

    @logical_types = {}

    {
      boolean: 1,
      tinyint: 2,
      smallint: 3,
      integer: 4,
      bigint: 5,
      utinyint: 6,
      usmallint: 7,
      uinteger: 8,
      ubigint: 9,
      float: 10,
      double: 11,
      timestamp: 12,
      date: 13,
      time: 14,
      interval: 15,
      hugeint: 16,
      uhugeint: 32,
      varchar: 17,
      blob: 18,
      # decimal: 19,
      timestamp_s: 20,
      timestamp_ms: 21,
      timestamp_ns: 22,
      # enum: 23,
      # list: 24,
      # struct: 25,
      # map: 26,
      # array: 33,
      # uuid: 27,
      # union: 28,
      uuid: 27,
      bit: 29,
      time_tz: 30,
      timestamp_tz: 31,
      any: 34,
      bignum: 35,
      sqlnull: 36,
      string_literal: 37,
      integer_literal: 38
      # time_ns: 39
    }.each do |method_name, type_id|
      define_singleton_method(method_name) do
        @logical_types[type_id] ||= DuckDB::LogicalType.new(type_id)
      end
      const_set(method_name.upcase, send(method_name))
    end

    class << self
      def resolve(symbol)
        return symbol if symbol.is_a?(DuckDB::LogicalType)

        raise_resolve_error(symbol) unless symbol.respond_to?(:upcase)

        DuckDB::LogicalType.const_get(symbol.upcase)
      rescue NameError
        raise_resolve_error(symbol)
      end

      # Creates an array logical type with the given child type and size.
      #
      # The +type+ argument can be a symbol or a DuckDB::LogicalType instance.
      # The +size+ argument specifies the fixed size of the array.
      #
      #   require 'duckdb'
      #
      #   array_type = DuckDB::LogicalType.create_array(:integer, 3)
      #   array_type.type #=> :array
      #   array_type.child_type.type #=> :integer
      #   array_type.size #=> 3
      def create_array(type, size)
        _create_array_type(LogicalType.resolve(type), size)
      end

      # Creates a list logical type with the given child type.
      #
      # The +type+ argument can be a symbol or a DuckDB::LogicalType instance.
      #
      #   require 'duckdb'
      #
      #   list_type = DuckDB::LogicalType.create_list(:integer)
      #   list_type.type #=> :list
      #   list_type.child_type.type #=> :integer
      #
      #   nested_list = DuckDB::LogicalType.create_list(list_type)
      #   nested_list.child_type.type #=> :list
      def create_list(type)
        _create_list_type(LogicalType.resolve(type))
      end

      # Creates a map logical type with the given key and value types.
      #
      # The +key_type+ and +value_type+ arguments can be symbols or
      # DuckDB::LogicalType instances.
      #
      #   require 'duckdb'
      #
      #   map_type = DuckDB::LogicalType.create_map(:integer, :varchar)
      #   map_type.type #=> :map
      #   map_type.key_type.type #=> :integer
      #   map_type.value_type.type #=> :varchar
      def create_map(key_type, value_type)
        _create_map_type(LogicalType.resolve(key_type), LogicalType.resolve(value_type))
      end

      # Creates a union logical type with the given member names and types.
      #
      # The keyword arguments map member names to types. Each type can be
      # a symbol or a DuckDB::LogicalType instance.
      #
      #   require 'duckdb'
      #
      #   union_type = DuckDB::LogicalType.create_union(num: :integer, str: :varchar)
      #   union_type.type #=> :union
      #   union_type.member_count #=> 2
      #   union_type.member_name_at(0) #=> "num"
      #   union_type.member_type_at(0).type #=> :integer
      def create_union(**members)
        resolved = members.transform_values { |v| LogicalType.resolve(v) }
        _create_union_type(resolved)
      end

      # Creates a struct logical type with the given member names and types.
      #
      # The keyword arguments map member names to types. Each type can be
      # a symbol or a DuckDB::LogicalType instance.
      #
      #   require 'duckdb'
      #
      #   struct_type = DuckDB::LogicalType.create_struct(name: :varchar, age: :integer)
      #   struct_type.type #=> :struct
      #   struct_type.child_count #=> 2
      #   struct_type.child_name_at(0) #=> "name"
      #   struct_type.child_type_at(0).type #=> :varchar
      def create_struct(**members)
        resolved = members.transform_values { |v| LogicalType.resolve(v) }
        _create_struct_type(resolved)
      end

      # Creates an enum logical type with the given members.
      #
      # Each member must be a String representing an enum member.
      #
      #   require 'duckdb'
      #
      #   enum_type = DuckDB::LogicalType.create_enum('happy', 'sad', 'neutral')
      #   enum_type.type #=> :enum
      #   enum_type.dictionary_size #=> 3
      #   enum_type.dictionary_value_at(0) #=> "happy"
      def create_enum(*members)
        _create_enum_type(members.map(&:to_s))
      end

      private

      def raise_resolve_error(symbol)
        raise DuckDB::Error, "Unknown logical type: `#{symbol.inspect}`"
      end
    end

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
      return to_enum(__method__) { member_count } unless block_given?

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
      return to_enum(__method__) { member_count } unless block_given?

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
      return to_enum(__method__) { child_count } unless block_given?

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
      return to_enum(__method__) { child_count } unless block_given?

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
      return to_enum(__method__) { dictionary_size } unless block_given?

      dictionary_size.times do |i|
        yield dictionary_value_at(i)
      end
    end

    # :nodoc:
    def inspect
      "<#{self.class}::#{type.upcase}>"
    end

    # :nodoc:
    def to_s
      inspect
    end
  end
end
