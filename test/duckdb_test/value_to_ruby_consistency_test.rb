# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  # DuckDB::Value#to_ruby must return the same Ruby object a query result
  # produces for the same value. Both paths share the vector reader, so
  # this test guards against them drifting apart again.
  class ValueToRubyConsistencyTest < Minitest::Test
    LITERALS = {
      boolean: 'true',
      tinyint: '42::TINYINT',
      smallint: '42::SMALLINT',
      integer: '42::INTEGER',
      bigint: '42::BIGINT',
      hugeint: '123456789012345678901234567890::HUGEINT',
      utinyint: '42::UTINYINT',
      usmallint: '42::USMALLINT',
      uinteger: '42::UINTEGER',
      ubigint: '42::UBIGINT',
      uhugeint: '123456789012345678901234567890::UHUGEINT',
      float: '1.5::FLOAT',
      double: '1.5::DOUBLE',
      decimal: '123.45::DECIMAL(10, 2)',
      varchar: "'héllo'::VARCHAR",
      blob: "'\\xAA\\x00'::BLOB",
      date: "'2024-01-02'::DATE",
      time: "'12:34:56.789'::TIME",
      timestamp: "'2024-01-02 03:04:05.123456'::TIMESTAMP",
      timestamp_s: "'2024-01-02 03:04:05'::TIMESTAMP_S",
      timestamp_ms: "'2024-01-02 03:04:05.123'::TIMESTAMP_MS",
      timestamp_ns: "'2024-01-02 03:04:05.123456789'::TIMESTAMP_NS",
      timestamp_tz: "'2024-01-02 03:04:05+09'::TIMESTAMPTZ",
      time_tz: "'12:34:56+09'::TIMETZ",
      interval: 'INTERVAL 3 DAY',
      uuid: "'6ba7b810-9dad-11d1-80b4-00c04fd430c8'::UUID",
      enum: "'x'::ENUM('x', 'y')",
      bit: "'101'::BIT",
      bignum: '123456789012345678901234567890123456789::BIGNUM',
      negative_bignum: '(-123456789012345678901234567890123456789)::BIGNUM',
      list: '[1, 2, 3]',
      array: '[1, 2]::INTEGER[2]',
      struct: "{'a': 1, 'b': 'x'}",
      map: "MAP {'k': 2}",
      union: '1::UNION(i INTEGER, s VARCHAR)',
      nested: "[{'a': [1, NULL]}]"
    }.freeze

    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
    end

    def teardown
      @con.close
      @db.close
    end

    LITERALS.each do |name, literal|
      define_method(:"test_#{name}") do
        expected = @con.query("SELECT #{literal}").first.first
        actual = fold(literal, name)

        assert_same_ruby_value(expected, actual)
      end
    end

    private

    # Evaluates the literal as a constant expression at bind time and
    # converts the resulting DuckDB::Value to Ruby.
    def fold(literal, name)
      value = nil
      func_name = "fold_#{name}"
      sf = DuckDB::ScalarFunction.new
      sf.name = func_name
      sf.return_type = :integer
      sf.add_parameter(:any)
      sf.set_function { |_| 1 }
      sf.set_bind { |b| value = b.get_argument(0).fold(b.client_context) }
      @con.register_scalar_function(sf)
      @con.execute("SELECT #{func_name}(#{literal})")
      value
    end

    def assert_same_ruby_value(expected, actual)
      refute_nil expected

      assert_equal expected.class, actual.class
      assert_equal expected, actual
      assert_equal expected.encoding, actual.encoding if expected.is_a?(String)
    end
  end
end
