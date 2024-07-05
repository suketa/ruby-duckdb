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

      module_function

      def statement_type_to_sym(val)
        raise DuckDB::Error, "Unknown statement type: #{val}" if val >= STATEMENT_TYPES.size

        STATEMENT_TYPES[val]
      end
    end
  end
end
