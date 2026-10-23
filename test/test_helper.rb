# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path('../lib', __dir__)
require 'duckdb'

require_relative 'duckdb_test/duckdb_version'

GC.verify_compaction_references(expand_heap: true, toward: :empty)

module DuckDBTest
  def duckdb_library_version
    Gem::Version.new(DuckDB::LIBRARY_VERSION)
  end

  # .release drops the prerelease part, so 2.0.0-dev counts as 2.0.0.
  def duckdb_2_0_or_later?
    duckdb_library_version.release >= Gem::Version.new('2.0.0')
  end

  module_function :duckdb_library_version, :duckdb_2_0_or_later?
end

require 'minitest/autorun'
