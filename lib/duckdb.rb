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
require 'duckdb/function_info'
require 'duckdb/vector'
require 'duckdb/data_chunk'
require 'duckdb/table_function'
require 'duckdb/table_function/bind_info'
require 'duckdb/table_function/init_info'
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
      case name
      when :BindInfo
        warn 'DuckDB::BindInfo is deprecated. Use DuckDB::TableFunction::BindInfo instead.', uplevel: 1
        const_set(:BindInfo, DuckDB::TableFunction::BindInfo)
      when :InitInfo
        warn 'DuckDB::InitInfo is deprecated. Use DuckDB::TableFunction::InitInfo instead.', uplevel: 1
        const_set(:InitInfo, DuckDB::TableFunction::InitInfo)
      else
        super
      end
    end
  end

  # Default to local time to preserve existing behavior unless explicitly
  # configured otherwise.
  self.default_timezone = :local
end
