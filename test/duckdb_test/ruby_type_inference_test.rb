# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  # Documents how an untyped Ruby object is mapped to a DuckDB type by
  # PreparedStatement#bind and Appender#append. Both use the same
  # inference module; the rows where the two columns differ are intended
  # (bind converts for convenience, append favours insert speed).
  class RubyTypeInferenceTest < Minitest::Test
    INFERENCE = DuckDB.const_get(:RubyTypeInference)

    BLOB = DuckDB::Blob.new("\xAA".b)
    TIME = Time.new(2024, 3, 15, 10, 30, 45)
    DATE = Date.new(2024, 3, 15)
    INTERVAL = DuckDB::Interval.new(interval_days: 3)

    # [value, bind tag, append tag]
    MATRIX = {
      nil: [nil, :null, :null],
      float: [1.5, :double, :double],
      small_integer: [42, :bigint, :smallint],
      int16_max: [32_767, :bigint, :smallint],
      int16_max_plus_one: [32_768, :bigint, :integer],
      int16_min: [-32_768, :bigint, :smallint],
      int16_min_minus_one: [-32_769, :bigint, :integer],
      int32_integer: [2**20, :bigint, :integer],
      int32_max: [(2**31) - 1, :bigint, :integer],
      int32_max_plus_one: [2**31, :bigint, :bigint],
      int32_min_minus_one: [-2**31 - 1, :bigint, :bigint],
      int64_integer: [2**40, :bigint, :bigint],
      int64_max: [(2**63) - 1, :bigint, :bigint],
      int64_max_plus_one: [2**63, :integer_as_varchar, :hugeint],
      int64_min: [-2**63, :bigint, :bigint],
      int64_min_minus_one: [-2**63 - 1, :integer_as_varchar, :hugeint],
      huge_integer: [2**70, :integer_as_varchar, :hugeint],
      string: ['abc', :varchar, :varchar],
      binary_string: ["\xAA".b, :blob, :blob],
      blob: [BLOB, :blob, :blob],
      true_value: [true, :boolean, :boolean],
      false_value: [false, :boolean, :boolean],
      date_time: [DateTime.new(2024, 3, 15, 1, 2, 3), :date_as_varchar, :date],
      time: [TIME, :time_as_varchar, :timestamp],
      date: [DATE, :date_as_varchar, :date],
      big_decimal: [BigDecimal('1.23'), :decimal, nil],
      interval: [INTERVAL, nil, :interval],
      array: [[1], nil, nil],
      symbol: [:sym, nil, nil]
    }.freeze

    # The append column is also checked end-to-end through Appender#append
    # in appender_test.rb (integer range boundaries, strings/blobs, nil,
    # Date and Time); see the tests below for Interval and BigDecimal.
    MATRIX.each do |name, (value, bind_tag, append_tag)|
      define_method(:"test_infer_#{name}") do
        assert_equal [bind_tag, append_tag], [INFERENCE.for_bind(value), INFERENCE.for_append(value)]
      end
    end

    def test_constant_is_private
      assert_raises(NameError) { DuckDB::RubyTypeInference }
    end

    # The DuckDB type PreparedStatement#bind sends, observed through
    # the type of an untyped `SELECT ?`.
    # nil is left out: the type DuckDB gives an untyped NULL parameter
    # differs by version (INTEGER before 2.0, SQLNULL from 2.0).
    BIND_RESULT_TYPES = {
      float: [1.5, :double],
      integer: [42, :bigint],
      huge_integer: [2**70, :varchar],
      string: ['abc', :varchar],
      blob: [BLOB, :blob],
      true_value: [true, :boolean],
      time: [TIME, :varchar],
      date: [DATE, :varchar],
      big_decimal: [BigDecimal('1.23'), :decimal]
    }.freeze

    BIND_RESULT_TYPES.each do |name, (value, type)|
      define_method(:"test_bind_#{name}_as_#{type}") do
        with_connection do |con|
          stmt = con.prepare('SELECT ?')
          stmt.bind(1, value)

          assert_equal type, stmt.execute.columns.first.type
        end
      end
    end

    def test_bind_nil_binds_null
      with_connection do |con|
        stmt = con.prepare('SELECT ?')
        stmt.bind(1, nil)

        assert_nil stmt.execute.first.first
      end
    end

    def test_bind_interval_is_not_supported
      with_connection do |con|
        stmt = con.prepare('SELECT ?')

        assert_raises(DuckDB::Error) { stmt.bind(1, INTERVAL) }
      end
    end

    def test_append_interval
      with_connection do |con|
        con.execute('CREATE TABLE t (i INTERVAL)')
        con.appender('t') { |a| a.append_row(INTERVAL) }

        assert_equal INTERVAL, con.query('SELECT i FROM t').first.first
      end
    end

    def test_append_big_decimal_is_not_supported
      with_connection do |con|
        con.execute('CREATE TABLE t (d DECIMAL(10, 2))')
        appender = con.appender('t')

        assert_raises(DuckDB::Error) { appender.append(BigDecimal('1.23')) }
      ensure
        appender&.close
      end
    end

    private

    def with_connection
      db = DuckDB::Database.open
      con = db.connect
      yield con
    ensure
      con&.close
      db&.close
    end
  end
end
