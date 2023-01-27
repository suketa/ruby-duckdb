module DuckDB
  LIBRARY_VERSION = library_version[1..] if DuckDB.methods.include?(:library_version)
end
