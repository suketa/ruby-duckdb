# frozen_string_literal: true

require 'bigdecimal'
require 'date'

module DuckDB
  # Decides which DuckDB type an untyped Ruby object becomes when it is
  # passed to PreparedStatement#bind (#for_bind) or Appender#append
  # (#for_append).
  #
  # Both decision tables live here, side by side, so that every difference
  # between the two is visible in one place. The differences are kept as
  # they are for now (they are part of the current public behaviour) and
  # are marked with "DIFF"; converging them is a separate, breaking change.
  #
  # #for_bind and #for_append return a type tag, or nil when the value is
  # not supported. Each caller maps the tag to its own bind_* / append_*
  # method.
  module RubyTypeInference # :nodoc: all
    # Ruby class => [tag for bind, tag for append]. Looked up by the exact
    # class first, then with `===` in order (so DateTime matches Date).
    # Integer and String depend on the value and are handled separately.
    TABLE = {
      NilClass => %i[null null],
      Float => %i[double double],
      TrueClass => %i[boolean boolean],
      FalseClass => %i[boolean boolean],
      # DIFF: bind sends Time/Date as VARCHAR, append as TIMESTAMP/DATE.
      Time => %i[time_as_varchar timestamp],
      Date => %i[date_as_varchar date],
      # DIFF: BigDecimal is supported by bind only.
      BigDecimal => [:decimal, nil],
      # DIFF: DuckDB::Interval is supported by append only.
      DuckDB::Interval => [nil, :interval]
    }.freeze

    # One hash per caller, so each lookup is a single fetch.
    BIND_TABLE = TABLE.transform_values(&:first).freeze
    APPEND_TABLE = TABLE.transform_values(&:last).freeze

    module_function

    # DIFF: bind uses BIGINT for every Integer in the int64 range and
    # VARCHAR beyond it; append (below) picks the smallest of
    # SMALLINT/INTEGER/BIGINT and HUGEINT beyond int64.
    def for_bind(value)
      case value
      when Integer then Converter::RANGE_INT64.cover?(value) ? :bigint : :integer_as_varchar
      when String then string_type(value)
      else BIND_TABLE[value.class] || subclass_tag(BIND_TABLE, value)
      end
    end

    # Appender#append matches Integer and String itself before calling
    # this (for speed); a change to those two branches must be made there too.
    def for_append(value)
      case value
      when Integer # duplicated in Appender#append_integer_value; change both
        if Converter::RANGE_INT16.cover?(value) then :smallint
        elsif Converter::RANGE_INT32.cover?(value) then :integer
        elsif Converter::RANGE_INT64.cover?(value) then :bigint
        else :hugeint
        end
      when String then string_type(value) # duplicated in Appender#append_string_value; change both
      else APPEND_TABLE[value.class] || subclass_tag(APPEND_TABLE, value)
      end
    end

    def string_type(value)
      if value.instance_of?(DuckDB::Blob) || value.encoding == Encoding::BINARY
        :blob
      else
        :varchar
      end
    end

    # For subclasses, and for classes whose tag is nil (not supported).
    def subclass_tag(table, value)
      # Same matching as a `case value when Klass` (the dispatch this replaced).
      table.find { |klass, _| klass === value }&.last # rubocop:disable Style/CaseEquality
    end

    private_class_method :string_type, :subclass_tag
  end

  private_constant :RubyTypeInference
end
