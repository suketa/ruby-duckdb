# frozen_string_literal: true

module DuckDB
  # DuckDB::TableNameParser provides shared table name parsing for classes that
  # accept a table name argument, such as DuckDB::Appender and DuckDB::TableDescription.
  #
  # It handles quoting: <tt>'"a.b"'</tt> or <tt>"'a.b'"</tt> strips the quotes and treats the name literally.
  # A name containing a dot (e.g. <tt>'a.b'</tt>) is used as-is, as a literal table name; it is
  # never split into schema and table. Use the +schema:+ and +catalog:+ keyword arguments to qualify a table.
  module TableNameParser
    private

    # Parses +table+, +schema+, and +catalog+, handling quoting.
    # Returns <tt>[table, schema, catalog]</tt>.
    def parse_table_name(table, schema, catalog)
      table = unquote_table_name(table) if quoted_table_name?(table)
      [table, schema, catalog]
    end

    def quoted_table_name?(name) # :nodoc:
      name.match?(/\A(["']).*\1\z/)
    end

    def unquote_table_name(name) # :nodoc:
      name[1..-2]
    end
  end
end
