module DuckDBTest
  module DuckDBVersion
    def self.duckdb_version
      return @duckdb_version if @duckdb_version

      DuckDB::Database.open('version.duckdb') do |db|
        db.connect do |con|
          r = con.query('SELECT VERSION();')
          @duckdb_version = Gem::Version.new(r.first.first.sub('v', ''))
        end
      end
    ensure
      FileUtils.rm_f('version.duckdb')
    end
  end
end
