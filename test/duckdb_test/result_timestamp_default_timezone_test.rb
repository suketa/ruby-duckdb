# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ResultTimestampDefaultTimezoneTest < Minitest::Test
    def setup
      @original_tz = ENV.fetch('TZ', nil)
      @original_default_timezone = DuckDB.default_timezone
      @db = DuckDB::Database.open
      @conn = @db.connect
      ENV['TZ'] = 'Europe/Berlin'
    end

    def teardown
      @conn.execute('DROP TABLE test;')
      @conn.close
      @db.close
      DuckDB.default_timezone = @original_default_timezone
      ENV['TZ'] = @original_tz
    end

    def test_timestamp_without_tz_uses_local_by_default
      @conn.execute('CREATE TABLE test (value TIMESTAMP);')
      @conn.execute("INSERT INTO test VALUES ('2019-01-02 12:34:56.123456');")
      result = @conn.execute('SELECT value FROM test;')
      time = result.each.to_a.first.first

      assert_equal(Time.local(2019, 1, 2, 12, 34, 56, 123_456), time)
      refute_predicate(time, :utc?)
    end

    def test_timestamp_without_tz_uses_utc_when_configured
      DuckDB.default_timezone = :utc

      @conn.execute('CREATE TABLE test (value TIMESTAMP);')
      @conn.execute("INSERT INTO test VALUES ('2019-01-02 12:34:56.123456');")
      result = @conn.execute('SELECT value FROM test;')
      time = result.each.to_a.first.first

      assert_equal(Time.utc(2019, 1, 2, 12, 34, 56, 123_456), time)
      assert_predicate(time, :utc?)
    end

    def test_timestamp_ms_respects_default_timezone_utc
      DuckDB.default_timezone = :utc

      @conn.execute('CREATE TABLE test (value TIMESTAMP_MS);')
      @conn.execute("INSERT INTO test VALUES ('2019-01-02 12:34:56.123456789');")
      result = @conn.execute('SELECT value FROM test;')
      time = result.each.to_a.first.first

      # TIMESTAMP_MS has millisecond resolution.
      assert_equal(Time.utc(2019, 1, 2, 12, 34, 56, 123_000), time)
      assert_predicate(time, :utc?)
    end

    def test_timestamp_ns_respects_default_timezone_utc
      DuckDB.default_timezone = :utc

      @conn.execute('CREATE TABLE test (value TIMESTAMP_NS);')
      @conn.execute("INSERT INTO test VALUES ('2019-01-02 12:34:56.123456789');")
      result = @conn.execute('SELECT value FROM test;')
      time = result.each.to_a.first.first

      # TIMESTAMP_NS has nanosecond resolution; Ruby Time stores microseconds.
      assert_equal(Time.utc(2019, 1, 2, 12, 34, 56, 123_456), time)
      assert_predicate(time, :utc?)
    end

    def test_time_without_tz_respects_default_timezone_utc
      DuckDB.default_timezone = :utc

      @conn.execute('CREATE TABLE test (value TIME);')
      @conn.execute("INSERT INTO test VALUES ('12:34:56.123456');")
      result = @conn.execute('SELECT value FROM test;')
      time = result.each.to_a.first.first

      assert_equal(12, time.hour)
      assert_equal(34, time.min)
      assert_equal(56, time.sec)
      assert_equal(123_456, time.usec)
      assert_predicate(time, :utc?)
    end
  end
end
