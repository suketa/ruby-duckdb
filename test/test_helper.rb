# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path('../lib', __dir__)
require 'duckdb'

require_relative 'duckdb_test/duckdb_version'

GC.verify_compaction_references(expand_heap: true, toward: :empty)

module DuckDBTest
  def duckdb_library_version
    Gem::Version.new(DuckDB::LIBRARY_VERSION)
  end

  module_function :duckdb_library_version
end

require 'minitest/autorun'
