# frozen_string_literal: true

module DuckDB
  module Converter
    module IntToSym
      STATEMENT_TYPES = %i[
        invalid
        select
        insert
        update
        explain
        delete
        prepare
        create
        execute
        alter
        transaction
        copy
        analyze
        variable_set
        create_func
        drop
        export
        pragma
        vacuum
        call
        set
        load
        relation
        extension
        logical_plan
        attach
        detach
        multi
      ].freeze

      HASH_TYPES = {

      }
	DUCKDB_TYPE_INVALID = 0,
	DUCKDB_TYPE_BOOLEAN = 1,
	DUCKDB_TYPE_TINYINT = 2,
	DUCKDB_TYPE_SMALLINT = 3,
	DUCKDB_TYPE_INTEGER = 4,
	DUCKDB_TYPE_BIGINT = 5,
	DUCKDB_TYPE_UTINYINT = 6,
	DUCKDB_TYPE_USMALLINT = 7,
	DUCKDB_TYPE_UINTEGER = 8,
	DUCKDB_TYPE_UBIGINT = 9,
	DUCKDB_TYPE_FLOAT = 10,
	DUCKDB_TYPE_DOUBLE = 11,
	DUCKDB_TYPE_TIMESTAMP = 12,
	DUCKDB_TYPE_DATE = 13,
	DUCKDB_TYPE_TIME = 14,
	DUCKDB_TYPE_INTERVAL = 15,
	DUCKDB_TYPE_HUGEINT = 16,
	DUCKDB_TYPE_UHUGEINT = 32,
	DUCKDB_TYPE_VARCHAR = 17,
	DUCKDB_TYPE_BLOB = 18,
	DUCKDB_TYPE_DECIMAL = 19,
	DUCKDB_TYPE_TIMESTAMP_S = 20,
	DUCKDB_TYPE_TIMESTAMP_MS = 21,
	DUCKDB_TYPE_TIMESTAMP_NS = 22,
	DUCKDB_TYPE_ENUM = 23,
	DUCKDB_TYPE_LIST = 24,
	DUCKDB_TYPE_STRUCT = 25,
	DUCKDB_TYPE_MAP = 26,
	DUCKDB_TYPE_ARRAY = 33,
	DUCKDB_TYPE_UUID = 27,
	DUCKDB_TYPE_UNION = 28,
	DUCKDB_TYPE_BIT = 29,
	DUCKDB_TYPE_TIME_TZ = 30,
	DUCKDB_TYPE_TIMESTAMP_TZ = 31,
      ]


      module_function

      def statement_type_to_sym(val)
        raise DuckDB::Error, "Unknown statement type: #{val}" if val >= STATEMENT_TYPES.size

        STATEMENT_TYPES[val]
      end
    end
  end
end
