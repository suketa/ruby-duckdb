module DuckDBTest
  class DuckDBVersion
    include Comparable

    def self.duckdb_version
      return @duckdb_version if @duckdb_version

      DuckDB::Database.open('version.duckdb') do |db|
        db.connect do |con|
          r = con.query('SELECT VERSION();')
          @duckdb_version = DuckDBVersion.new(r.first.first.sub('v', ''))
        end
      end
    ensure
      FileUtils.rm_f('version.duckdb')
    end

    # Ruby 2.6.X does not support comparing Gem::Version object with string.
    #   Gem::Version.new(x) >= '0.3.3' #=> Exception
    def initialize(str)
      @version = Gem::Version.new(str)
    end

    def <=>(other)
      @version <=> Gem::Version.new(other)
    end
  end
end
