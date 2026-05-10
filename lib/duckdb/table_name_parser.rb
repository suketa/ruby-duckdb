# frozen_string_literal: true

module DuckDB
  # DuckDB::TableNameParser provides shared table name parsing for classes that
  # accept a table name argument, such as DuckDB::Appender and DuckDB::TableDescription.
  #
  # It handles:
  # - Dot-notation: <tt>'schema.table'</tt> is split into schema and table (deprecated).
  # - Quoting: <tt>'"a.b"'</tt> or <tt>"'a.b'"</tt> strips the quotes and treats the name literally.
  module TableNameParser
    private

    # Parses +table+, +schema+, and +catalog+, handling quoting and dot-notation.
    # Returns <tt>[table, schema, catalog]</tt>.
    def parse_table_name(table, schema, catalog)
      if quoted_table_name?(table)
        [unquote_table_name(table), schema, catalog]
      elsif table.include?('.')
        apply_dot_notation(table, schema, catalog)
      else
        [table, schema, catalog]
      end
    end

    def quoted_table_name?(name) # :nodoc:
      name.match?(/\A(["']).*\1\z/)
    end

    def unquote_table_name(name) # :nodoc:
      name[1..-2]
    end

    def apply_dot_notation(table, schema, catalog) # :nodoc:
      parts = table.split('.')
      raise ArgumentError, "Too many dot-separated segments in '#{table}'" if parts.length > 3

      warn_dot_notation_deprecated(table)
      case parts.length
      when 2 then [parts[1], schema || parts[0], catalog]
      when 3 then [parts[2], schema || parts[1], catalog || parts[0]]
      else raise ArgumentError, "Unexpected segment count in '#{table}'"
      end
    end

    def warn_dot_notation_deprecated(table) # :nodoc:
      class_name = self.class.name
      warn(
        "Passing dot-notation '#{table}' to #{class_name}.new is deprecated. " \
        "If '#{table}' is a schema-qualified table, use #{class_name}.new(con, table, schema: schema) instead. " \
        "If '#{table}' is a literal table name containing a dot, " \
        "use #{class_name}.new(con, '\"#{table}\"') instead.",
        category: :deprecated
      )
    end
  end
end
