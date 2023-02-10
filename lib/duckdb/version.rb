module DuckDB
  # The version string of ruby-duckdb.
  # Currently, ruby-duckdb is NOT semantic versioning.
  VERSION = '0.6.1'.freeze
  LIBRARY_VERSION = library_version[1..] if defined? library_version
end
