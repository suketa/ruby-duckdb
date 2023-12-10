$LOAD_PATH.unshift File.expand_path('../lib', __dir__)
require 'duckdb'

require_relative './duckdb_test/duckdb_version'

if defined?(GC.verify_compaction_references) == 'method'
  if Gem::Version.new(RUBY_VERSION) >= Gem::Version.new('3.2.0')
    GC.verify_compaction_references(expand_heap: true, toward: :empty)
  else
    GC.verify_compaction_references(double_heap: true, toward: :empty)
  end
end

module DuckDBTest
  def duckdb_library_version
    Gem::Version.new(DuckDB::LIBRARY_VERSION)
  end

  module_function :duckdb_library_version
end

require 'minitest/autorun'
