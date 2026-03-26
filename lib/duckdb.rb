# frozen_string_literal: true

require 'duckdb/duckdb_native'
require 'duckdb/library_version'
require 'duckdb/version'
require 'duckdb/converter'
require 'duckdb/database'
require 'duckdb/connection'
require 'duckdb/extracted_statements'
require 'duckdb/result'
require 'duckdb/prepared_statement'
require 'duckdb/pending_result'
require 'duckdb/appender'
require 'duckdb/config'
require 'duckdb/column'
require 'duckdb/logical_type'
require 'duckdb/scalar_function'
require 'duckdb/scalar_function/bind_info'
require 'duckdb/vector'
require 'duckdb/data_chunk'
require 'duckdb/table_function'
require 'duckdb/table_function/bind_info'
require 'duckdb/table_function/init_info'
require 'duckdb/table_function/function_info'
require 'duckdb/infinity'
require 'duckdb/instance_cache'
require 'duckdb/casting'

# DuckDB provides Ruby interface of DuckDB.
module DuckDB
  class << self
    # Controls how DuckDB converts timestamp and time values without explicit
    # time zone information.
    #
    # - `:utc`   - interpret values as UTC
    # - `:local` - (default) interpret values as local time, preserving existing behavior
    #
    # Example:
    #   DuckDB.default_timezone = :utc
    #
    # This setting only affects conversion of values without time zone. Values
    # with explicit time zone are always interpreted according to their offset.
    attr_reader :default_timezone

    def default_timezone=(value)
      raise ArgumentError, 'DuckDB.default_timezone must be either :utc or :local.' unless %i[local utc].include?(value)

      @default_timezone = value
    end

    def const_missing(name)
      deprecated = DEPRECATED_CONSTANTS[name]
      return super unless deprecated

      warn "DuckDB::#{name} is deprecated. Use #{deprecated} instead.", uplevel: 1
      const_set(name, deprecated.split('::').reduce(Object) { |mod, part| mod.const_get(part) })
    end
  end

  DEPRECATED_CONSTANTS = {
    BindInfo: 'DuckDB::TableFunction::BindInfo',
    InitInfo: 'DuckDB::TableFunction::InitInfo',
    FunctionInfo: 'DuckDB::TableFunction::FunctionInfo'
  }.freeze

  # Default to local time to preserve existing behavior unless explicitly
  # configured otherwise.
  self.default_timezone = :local
end
