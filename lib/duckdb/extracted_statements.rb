# frozen_string_literal: true

module DuckDB
  class ExtractedStatements < ExtractedStatementsImpl
    include Enumerable

    def initialize(con, sql)
      @con = con
      super
    end

    def each
      return to_enum(__method__) { size } unless block_given?

      size.times do |i|
        yield prepared_statement(@con, i)
      end
    end
  end
end
