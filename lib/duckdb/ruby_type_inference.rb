# frozen_string_literal: true

require 'bigdecimal'
require 'date'

module DuckDB
  # :nodoc: all
  #
  # Decides which DuckDB type an untyped Ruby object becomes when it is
  # passed to PreparedStatement#bind (target +:bind+) or Appender#append
  # (target +:append+).
  #
  # Both decision tables live here, side by side, so that every difference
  # between the two is visible in one place. The differences are kept as
  # they are for now (they are part of the current public behaviour) and
  # are marked with "DIFF"; converging them is a separate, breaking change.
  #
  # #infer returns a type tag, or nil when the value is not supported.
  # Each caller maps the tag to its own bind_* / append_* method.
  module RubyTypeInference
    TARGET_INDEX = { bind: 0, append: 1 }.freeze

    # Ruby class => [tag for :bind, tag for :append]. Looked up by the exact
    # class first, then with is_a? in order (so DateTime matches Date).
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

    module_function

    def infer(value, target)
      infer_at(value, TARGET_INDEX.fetch(target) { raise ArgumentError, "unknown target #{target.inspect}" })
    end

    # Same as infer(value, :bind), without the target lookup.
    def for_bind(value)
      infer_at(value, 0)
    end

    # Same as infer(value, :append), without the target lookup (Appender#append is a hot path).
    def for_append(value)
      infer_at(value, 1)
    end

    def infer_at(value, column)
      case value
      when Integer then integer_type(value, column)
      when String then blob?(value) ? :blob : :varchar
      else
        tags = TABLE[value.class] || subclass_tags(value)
        tags && tags[column]
      end
    end

    def subclass_tags(value)
      TABLE.find { |klass, _| value.is_a?(klass) }&.last
    end

    def blob?(value)
      value.instance_of?(DuckDB::Blob) || value.encoding == Encoding::BINARY
    end

    # DIFF: bind uses BIGINT for every Integer in the int64 range and
    # VARCHAR beyond it; append picks the smallest of SMALLINT/INTEGER/BIGINT
    # and HUGEINT beyond int64.
    def integer_type(value, column)
      return Converter::RANGE_INT64.cover?(value) ? :bigint : :integer_as_varchar if column.zero?

      if Converter::RANGE_INT16.cover?(value) then :smallint
      elsif Converter::RANGE_INT32.cover?(value) then :integer
      elsif Converter::RANGE_INT64.cover?(value) then :bigint
      else :hugeint
      end
    end
  end

  private_constant :RubyTypeInference
end
