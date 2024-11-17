# frozen_string_literal: true

module DuckDB
  # represents the version of the DuckDB library.
  # If DuckDB.library_version is v0.2.0, then DuckDB::LIBRARY_VERSION is 0.2.0.
  LIBRARY_VERSION = library_version[1..]
end
