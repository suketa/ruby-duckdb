module DuckDB
  class Connection
    def query(sql, *args)
      return query_sql(sql) if args.empty?

      stmt = PreparedStatement.new(self, sql)
      args.each_with_index do |arg, i|
        stmt.bind(i + 1, arg)
      end
      stmt.execute
    end
  end
end
