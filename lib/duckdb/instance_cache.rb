# frozen_string_literal: true

if defined?(DuckDB::InstanceCache)

module DuckDB
  # The DuckDB::InstanceCache is necessary if a client/program (re)opens
  # multiple databases to the same file within the same statement.
  #
  #   require 'duckdb'
  #   cache = DuckDB::InstanceCache.new
  #   db1 = cache.get(path: 'db.duckdb')
  #   db2 = cache.get(path: 'db.duckdb')
  class InstanceCache
    def get(path: nil, config: nil)
      get_or_create(path, config)
    end
  end
end

end
