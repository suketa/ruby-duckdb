# frozen_string_literal: true

module DuckDB
  module Converter
    module IntToSym # :nodoc: all
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
        0 => :invalid,
        1 => :boolean,
        2 => :tinyint,
        3 => :smallint,
        4 => :integer,
        5 => :bigint,
        6 => :utinyint,
        7 => :usmallint,
        8 => :uinteger,
        9 => :ubigint,
        10 => :float,
        11 => :double,
        12 => :timestamp,
        13 => :date,
        14 => :time,
        15 => :interval,
        16 => :hugeint,
        32 => :uhugeint,
        17 => :varchar,
        18 => :blob,
        19 => :decimal,
        20 => :timestamp_s,
        21 => :timestamp_ms,
        22 => :timestamp_ns,
        23 => :enum,
        24 => :list,
        25 => :struct,
        26 => :map,
        33 => :array,
        27 => :uuid,
        28 => :union,
        29 => :bit,
        30 => :time_tz,
        31 => :timestamp_tz,
        35 => :bignum,
        36 => :sqlnull,
        37 => :string_literal,
        38 => :integer_literal
      }.freeze

      module_function

      def statement_type_to_sym(val) # :nodoc:
        raise DuckDB::Error, "Unknown statement type: #{val}" if val >= STATEMENT_TYPES.size

        STATEMENT_TYPES[val]
      end

      def type_to_sym(val) # :nodoc:
        raise DuckDB::Error, "Unknown type: #{val}" unless HASH_TYPES.key?(val)

        HASH_TYPES[val]
      end
    end
  end
end
