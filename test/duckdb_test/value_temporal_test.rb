# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ValueTemporalTest < Minitest::Test
    def test_create_date_with_date
      value = DuckDB::Value.create_date(Date.new(2026, 7, 12))

      assert_instance_of(DuckDB::Value, value)
      assert_equal(Date.new(2026, 7, 12), value.to_ruby)
    end

    def test_create_date_with_string
      assert_equal(Date.new(2026, 7, 12), DuckDB::Value.create_date('2026-07-12').to_ruby)
    end

    def test_create_date_with_time
      time = Time.local(2026, 7, 12, 1, 2, 3)

      assert_equal(Date.new(2026, 7, 12), DuckDB::Value.create_date(time).to_ruby)
    end

    def test_create_date_with_invalid_string_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_date('not a date') }
    end

    def test_create_date_with_nil_raises_argument_error
      assert_raises(ArgumentError) { DuckDB::Value.create_date(nil) }
    end
  end
end
