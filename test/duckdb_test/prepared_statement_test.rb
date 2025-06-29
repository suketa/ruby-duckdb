# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class PreparedStatementTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      @con.query(self.class.create_table_sql)
      @con.query(self.class.insert_sql)
    end

    def teardown
      @con.query('DROP TABLE IF EXISTS values')
      @con&.close
      @db&.close
    end

    def self.today
      @today ||= Date.today
    end

    def self.now
      @now ||= Time.now
    end

    def self.create_table_sql
      <<-SQL
      CREATE TABLE a (
        id INTEGER,
        col_boolean BOOLEAN,
        col_tinyint TINYINT,
        col_smallint SMALLINT,
        col_integer INTEGER,
        col_bigint BIGINT,
        col_hugeint HUGEINT,
        col_real REAL,
        col_double DOUBLE,
        col_decimal DECIMAL(9,4),
        col_varchar VARCHAR,
        col_date DATE,
        col_timestamp TIMESTAMP,
        col_time TIME,
        col_blob BLOB,
        col_interval INTERVAL
        );
      SQL
    end

    def self.insert_sql
      datestr = today.strftime('%Y-%m-%d')
      <<-SQL
      INSERT INTO a VALUES (
        1,
        True,
        127,
        32767,
        2147483647,
        9223372036854775807,
        170141183460469231731687303715884105727,
        12345.375,
        12345.6789,
        98765.4321,
        'str',
        '#{datestr}',
        '2019-11-09 12:34:56',
        '12:34:56.000001',
        'blob data',
        '1 year 2 months 3 days 12:34:56.987654'
      );
      SQL
    end

    def expected_row
      @expected ||= [
        1,
        true,
        127,
        32_767,
        2_147_483_647,
        9_223_372_036_854_775_807,
        170_141_183_460_469_231_731_687_303_715_884_105_727,
        12_345.375,
        12_345.6789,
        98_765.4321,
        'str',
        self.class.today,
        Time.parse('2019-11-09 12:34:56'),
        Time.local(self.class.now.year, self.class.now.month, self.class.now.day, 12, 34, 56, 1),
        'blob data',
        DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 45_296_987_654)
      ]
    end

    def test_class_exist
      assert_instance_of(Class, DuckDB::PreparedStatement)
    end

    def test_s_new
      assert_instance_of(DuckDB::PreparedStatement, DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a'))
      assert_raises(ArgumentError) { DuckDB::PreparedStatement.new(@con) }
      assert_raises(ArgumentError) { DuckDB::PreparedStatement.new }
      assert_raises(TypeError) { DuckDB::PreparedStatement.new(@con, 1) }
      assert_raises(TypeError) { DuckDB::PreparedStatement.new(1, 1) }
      assert_raises(DuckDB::Error) { DuckDB::PreparedStatement.new(@con, 'SELECT * FROM') }
    end

    def test_s_prepare_without_block
      assert_instance_of(DuckDB::PreparedStatement, DuckDB::PreparedStatement.prepare(@con, 'SELECT * FROM a'))
    end

    def test_s_prepare_with_block
      r = DuckDB::PreparedStatement.prepare(@con, 'SELECT * FROM a WHERE id = $1') do |stmt|
        stmt.bind(1, 1)
        stmt.execute
      end
      assert_equal(1, r.first.first)
    end

    def test_execute
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a')
      result = stmt.execute
      assert_instance_of(DuckDB::Result, result)

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a where id = ?')
      assert_raises(DuckDB::Error) { stmt.execute }
    end

    def test_nparams
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a')
      assert_equal(0, stmt.nparams)

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE id = $1')
      assert_equal(1, stmt.nparams)

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE id = ?')
      assert_equal(1, stmt.nparams)
    end

    def test_statement_type
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a')
      assert_equal(:select, stmt.statement_type)
    end

    def test_param_type
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE id = $1')
      assert_equal(:invalid, stmt.param_type(0))
      assert_equal(:integer, stmt.param_type(1))
      assert_equal(:invalid, stmt.param_type(2))
    end

    def test_clear_bindings
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE id = $1')
      stmt.bind(1, 1)
      stmt.clear_bindings
      res = assert_raises(DuckDB::Error) { stmt.execute }
      assert_match(/Values were not provided for the following prepared.*: 1/, res.message)

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE id = $id')
      stmt.bind(:id, 1)
      stmt.clear_bindings
      res = assert_raises(DuckDB::Error) { stmt.execute }
      assert_match(/Values were not provided for the following prepared.*: id/, res.message)
    end

    def test_pending_prepared
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a')
      pending = stmt.pending_prepared
      assert_instance_of(DuckDB::PendingResult, pending)
    end

    def test_bind_parameter_index
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE id = $id')

      assert_equal(1, stmt.bind_parameter_index('id'))

      stmt = DuckDB::PreparedStatement.new(
        @con,
        'SELECT * FROM a WHERE id = $id AND col_boolean = $col_boolean AND id = $id'
      )

      assert_equal(1, stmt.bind_parameter_index('id'))
      assert_equal(2, stmt.bind_parameter_index('col_boolean'))
    end

    def test_bind_parameter_name
      stmt = DuckDB::PreparedStatement.new(
        @con,
        'SELECT * FROM a WHERE id = $id AND col_boolean = $col_boolean AND id = $id'
      )

      assert_equal('id', stmt.parameter_name(1))
      assert_equal('col_boolean', stmt.parameter_name(2))

      assert_raises(ArgumentError) { stmt.parameter_name(0) }
      assert_raises(DuckDB::Error) { stmt.parameter_name(3) }
    end

    def test_bind_index_number_exception
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE id = $2')
      exception = assert_raises(DuckDB::Error) { stmt.bind(2, 1) }
      assert_equal('fail to bind 2 parameter', exception.message)
    end

    def test_bind_bool
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_boolean = $1')

      assert_raises(ArgumentError) { stmt.bind_bool(0, true) }
      assert_raises(DuckDB::Error) { stmt.bind_bool(2, true) }

      stmt.bind_bool(1, true)
      assert_equal(expected_row, stmt.execute.each.first)

      stmt.bind_bool(1, false)
      assert_nil(stmt.execute.each.first)

      assert_raises(ArgumentError) { stmt.bind_bool(1, 'True') }
    end

    def test_bind_int8
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_tinyint = $1')

      stmt.bind_int8(1, 127)
      assert_equal(expected_row, stmt.execute.each.first)
    end

    def test_bind_uint8
      value = (2**8) - 1
      @con.query('CREATE TABLE values (value UINT8)')

      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO values(value) VALUES ($1)')
      stmt.bind_uint8(1, value)
      stmt.execute

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM values WHERE value = $1')
      stmt.bind_uint8(1, value)
      assert_equal(value, stmt.execute.each.first[0])
    end

    def test_bind_uint8_with_negative
      @con.query('CREATE TABLE values (value UINT8)')

      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO values(value) VALUES ($1)')
      assert_raises(DuckDB::Error) { stmt.bind_uint8(1, -1) }
    end

    def test_bind_int16
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_smallint = $1')

      stmt.bind_int16(1, 32_767)
      assert_equal(expected_row, stmt.execute.each.first)

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_integer = $1')
      stmt.bind_int16(1, 32_767)
      assert_nil(stmt.execute.each.first)

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_bigint = $1')
      stmt.bind_int16(1, 32_767)
      assert_nil(stmt.execute.each.first)
    end

    def test_bind_uint16
      value = (2**16) - 1
      @con.query('CREATE TABLE values (value USMALLINT)')

      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO values(value) VALUES ($1)')
      stmt.bind_uint16(1, value)
      stmt.execute

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM values WHERE value = $1')
      stmt.bind_uint16(1, value)
      assert_equal(value, stmt.execute.each.first[0])
    end

    def test_bind_uint16_with_negative
      @con.query('CREATE TABLE values (value UINT16)')

      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO values(value) VALUES ($1)')
      assert_raises(DuckDB::Error) { stmt.bind_uint16(1, -1) }
    end

    def test_bind_int32
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_smallint = $1')

      stmt.bind_int32(1, 32_767)
      assert_equal(expected_row, stmt.execute.each.first)

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_integer = $1')
      stmt.bind_int32(1, 2_147_483_647)
      assert_equal(expected_row, stmt.execute.each.first)

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_bigint = $1')
      stmt.bind_int32(1, 2_147_483_647)
      assert_nil(stmt.execute.each.first)
    end

    def test_bind_uint32
      value = (2**32) - 1
      @con.query('CREATE TABLE values (value UINTEGER)')

      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO values(value) VALUES ($1)')
      stmt.bind_uint32(1, value)
      stmt.execute

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM values WHERE value = $1')
      stmt.bind_uint32(1, value)
      assert_equal(value, stmt.execute.each.first[0])
    end

    def test_bind_uint32_with_negative
      @con.query('CREATE TABLE values (value UINTEGER)')

      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO values(value) VALUES ($1)')
      assert_raises(DuckDB::Error) { stmt.bind_uint32(1, -1) }
    end

    def test_bind_int64
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_smallint = $1')

      stmt.bind_int64(1, 32_767)
      assert_equal(expected_row, stmt.execute.each.first)

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_integer = $1')
      stmt.bind_int64(1, 2_147_483_647)
      assert_equal(expected_row, stmt.execute.each.first)

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_bigint = $1')
      stmt.bind_int64(1, 9_223_372_036_854_775_807)
      assert_equal(expected_row, stmt.execute.each.first)
    end

    def test_bind_uint64
      value = (2**64) - 1
      @con.query('CREATE TABLE values (value UBIGINT)')

      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO values(value) VALUES ($1)')
      stmt.bind_uint64(1, value)
      stmt.execute

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM values WHERE value = $1')
      stmt.bind_uint64(1, value)
      assert_equal(value, stmt.execute.each.first[0])
    end

    def test_bind_uint64_with_negative
      @con.query('CREATE TABLE values (value UBIGINT)')

      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO values(value) VALUES ($1)')
      assert_raises(DuckDB::Error) { stmt.bind_uint64(1, -1) }
    end

    def test__bind_hugeint
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_hugeint = $1')
      stmt.send(:_bind_hugeint, 1, 18_446_744_073_709_551_615, 9_223_372_036_854_775_807)
      assert_equal(expected_row, stmt.execute.each.first)
    end

    def test__bind_hugeint_internal
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_hugeint = $1')
      stmt.bind_hugeint_internal(1, 170_141_183_460_469_231_731_687_303_715_884_105_727)
      assert_equal(expected_row, stmt.execute.each.first)
    end

    def test_bind_hugeint
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_smallint = $1')

      stmt.bind_hugeint(1, 32_767)
      assert_equal(expected_row, stmt.execute.each.first)

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_integer = $1')
      stmt.bind_hugeint(1, 2_147_483_647)
      assert_equal(expected_row, stmt.execute.each.first)

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_bigint = $1')
      stmt.bind_hugeint(1, 9_223_372_036_854_775_807)
      assert_equal(expected_row, stmt.execute.each.first)

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_hugeint = $1')
      stmt.bind_hugeint(1, 170_141_183_460_469_231_731_687_303_715_884_105_727)
      assert_equal(expected_row, stmt.execute.each.first)

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_hugeint = $1')
      e = assert_raises(ArgumentError) { stmt.bind_hugeint(1, 1.5) }
      assert_equal('2nd argument `1.5` must be Integer.', e.message)
    end

    def test_bind_uhugeint
      value = (2**128) - 1
      @con.query('CREATE TABLE values (value UHUGEINT)')

      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO values(value) VALUES ($1)')
      stmt.bind_uhugeint(1, value)
      stmt.execute

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM values WHERE value = $1')
      stmt.bind_uhugeint(1, value)
      assert_equal(value, stmt.execute.each.first[0])
    end

    def test_bind_float
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_real = $1')

      assert_raises(ArgumentError) { stmt.bind_float(0, 12_345.375) }
      assert_raises(DuckDB::Error) { stmt.bind_float(2, 12_345.375) }

      stmt.bind_float(1, 12_345.375)
      assert_equal(expected_row, stmt.execute.each.first)

      assert_raises(TypeError) { stmt.bind_float(1, 'invalid_float_val') }
    end

    def test_bind_double
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_double = $1')

      assert_raises(ArgumentError) { stmt.bind_double(0, 12_345.6789) }
      assert_raises(DuckDB::Error) { stmt.bind_double(2, 12_345.6789) }

      stmt.bind_double(1, 12_345.6789)
      assert_equal(expected_row, stmt.execute.each.first)

      assert_raises(TypeError) { stmt.bind_double(1, 'invalid_double_val') }
    end

    def test__bind_decimal
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_decimal = $1')
      stmt.send(:_bind_decimal, 1, 987654321, 0, 9, 4)
      assert_equal(expected_row, stmt.execute.each.first)
    end

    def test_bind_decimal
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_decimal = $1')

      assert_raises(ArgumentError) { stmt.bind_decimal(0, BigDecimal('98765.4321')) }
      assert_raises(DuckDB::Error) { stmt.bind_decimal(2, BigDecimal('98765.4321')) }

      e = assert_raises(ArgumentError) { stmt.bind_decimal(1, 'parse_error') }
      assert_equal('Cannot parse `"parse_error"` to BigDecimal object. invalid value for BigDecimal(): "parse_error"', e.message)
      e = assert_raises(ArgumentError) { stmt.bind_decimal(1, BigDecimal('Infinity')) }
      assert_equal('The argument `Infinity` must be converted to Integer. Computation results in \'Infinity\'', e.message)

      stmt.bind_decimal(1, 98765.4321)
      assert_equal(expected_row, stmt.execute.each.first)

      stmt.bind_decimal(1, BigDecimal('98765.4321'))
      assert_equal(expected_row, stmt.execute.each.first)
    end

    def test_bind_varchar
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_varchar = $1')

      assert_raises(ArgumentError) { stmt.bind_varchar(0, 'str') }
      assert_raises(DuckDB::Error) { stmt.bind_varchar(2, 'str') }
      stmt.bind_varchar(1, 'str')
      result = stmt.execute
      assert_equal(1, result.each.first[0])

      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_varchar = ?')

      assert_raises(ArgumentError) { stmt.bind_varchar(0, 'str') }
      assert_raises(DuckDB::Error) { stmt.bind_varchar(2, 'str') }

      stmt.bind_varchar(1, 'str')
      result = stmt.execute
      assert_equal(1, result.each.first[0])

      # SQL injection
      param = "' or 1 = 1 --"
      result = @con.query("SELECT * FROM a WHERE col_varchar = '#{param}'")
      assert_equal(expected_row, result.each.first)

      # block SQL injection using bind_varchar
      stmt.bind_varchar(1, param)
      result = stmt.execute
      assert_nil(result.each.first)
    end

    class Foo
      def to_s
        raise 'not implemented to_s'
      end
    end

    class Bar
      def to_str
        today = PreparedStatementTest.today
        day = today.day
        suffix = if day.between?(11, 13)
                   'th'
                 else
                   { 1 => 'st', 2 => 'nd', 3 => 'rd' }.fetch(day % 10, 'th')
                 end

        "#{today.strftime('%B')}, #{day}#{suffix}, #{today.year}"
      end
    end

    def test_bind_varchar_error
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_varchar = $1')
      assert_raises(TypeError) { stmt.bind_varchar(1, Foo.new) }
    end

    def test_bind_varchar_date
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_date = $1')

      stmt.bind_varchar(1, PreparedStatementTest.today.strftime('%Y-%m-%d'))
      result = stmt.execute
      assert_equal(1, result.each.first[0])
    end

    def test_bind_varchar_date_with_invalid_timestamp_string
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_date = $1')

      stmt.bind_varchar(1, 'invalid_date_string')
      assert_raises(DuckDB::Error) { stmt.execute }
    end

    def test_bind_varchar_timestamp
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_timestamp = $1')

      stmt.bind_varchar(1, '2019/11/09 12:34:56')
      result = stmt.execute
      assert_equal(1, result.each.first[0])
    end

    def test_bind_varchar_timestamp_with_invalid_timestamp_string
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_timestamp = $1')

      stmt.bind_varchar(1, 'invalid_timestamp_string')
      assert_raises(DuckDB::Error) { stmt.execute }
    end

    def test_bind_blob
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO a(id, col_blob) VALUES (NULL, $1)')
      stmt.bind_blob(1, DuckDB::Blob.new("\0\1\2\3\4\5"))
      assert_instance_of(DuckDB::Result, stmt.execute)
      result = @con.execute('SELECT col_blob FROM a WHERE id IS NULL')
      assert_equal("\0\1\2\3\4\5".encode(Encoding::BINARY), result.first.first)
    end

    def test_bind_null
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO a(id) VALUES ($1)')
      stmt.bind_null(1)
      assert_instance_of(DuckDB::Result, stmt.execute)
      r = @con.query('SELECT * FROM a WHERE id IS NULL')
      assert_nil(r.each.first.first)
    end

    def test_bind_date
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_date = $1')
      today = PreparedStatementTest.today

      stmt.bind_date(1, today)
      result = stmt.execute
      assert_equal(1, result.each.first[0])

      stmt.bind_date(1, Time.now)
      result = stmt.execute
      assert_equal(1, result.each.first[0])

      date_str = Bar.new.to_str
      stmt.bind_date(1, date_str)
      result = stmt.execute
      assert_equal(1, result.each.first[0])

      stmt.bind_date(1, Bar.new)
      result = stmt.execute
      assert_equal(1, result.each.first[0])

      e = assert_raises(ArgumentError) { stmt.bind_date(1, Foo.new) }
      assert_match(/Cannot parse `#<DuckDBTest::PreparedStatementTest::Foo/, e.message)
    end

    def test__bind_date
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_date = $1')

      today = PreparedStatementTest.today
      stmt.send(:_bind_date, 1, today.year, today.month, today.day)
      result = stmt.execute
      assert_equal(1, result.each.first[0])
    end

    def test_bind_time
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_time = $1')

      col_time = @con.query('SELECT col_time from a').first.first

      bind_val = Time.local(1970, 1, 1, 12, 34, 56, 1)
      stmt.bind_time(1, bind_val)
      result = stmt.execute

      # fix for using duckdb_fetch_chunk in Result#chunk_each
      result = result.to_a

      dump_now = "data=#{col_time}, data.usec=#{col_time.usec} bind_val=#{bind_val}, bind_val.usec=#{bind_val.usec}"
      assert_instance_of(Array, result.each.first, dump_now)
      assert_equal(1, result.each.first[0])

      stmt.bind_time(1, bind_val.strftime('%F %T.%N'))
      result = stmt.execute

      # fix for using duckdb_fetch_chunk in Result#chunk_each
      result = result.to_a

      assert_instance_of(Array, result.each.first, dump_now)
      assert_equal(1, result.each.first[0])

      e = assert_raises(ArgumentError) { stmt.bind_time(1, Foo.new) }
      assert_match(/Cannot parse `#<DuckDBTest::PreparedStatementTest::Foo/, e.message)
    end

    def test__bind_time
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_time = $1')

      stmt.send(:_bind_time, 1, 12, 34, 56, 1)

      result = stmt.execute

      # fix for using duckdb_fetch_chunk in Result#chunk_each
      result = result.to_a

      assert_instance_of(Array, result.each.first)
      assert_equal(1, result.each.first[0])
    end

    def test__bind_timestamp
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_timestamp = $1')

      stmt.send(:_bind_timestamp, 1, 2019, 11, 9, 12, 34, 56, 0)
      result = stmt.execute
      assert_equal(1, result.each.first[0])
    end

    def test_bind_timestamp
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_timestamp = $1')

      stmt.bind_timestamp(1, Time.new(2019, 11, 9, 12, 34, 56))
      result = stmt.execute
      assert_equal(1, result.each.first[0])
    end

    def test__bind_itnerval
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_interval = $1')

      micros = (((12 * 3600) + (34 * 60) + 56) * 1_000_000) + 987_654
      stmt.send(:_bind_interval, 1, 14, 3, micros)
      result = stmt.execute
      assert_equal(1, result.each.first[0])
    end

    def test_bind_interval
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_interval = $1')

      stmt.bind_interval(1, 'P1Y2M3DT12H34M56.987654S')
      result = stmt.execute
      assert_equal(1, result.each.first[0])
    end

    def test_bind_with_boolean
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_boolean = $1')

      stmt.bind(1, true)
      assert_equal(expected_row, stmt.execute.each.first)

      stmt.bind(1, false)
      assert_nil(stmt.execute.each.first)
    end

    def test_bind_with_int16
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_smallint = $1')

      stmt.bind(1, 1)
      assert_nil(stmt.execute.each.first)

      stmt.bind(1, 32_767)
      assert_equal(expected_row, stmt.execute.each.first)
    end

    def test_bind_with_int32
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_integer = $1')

      stmt.bind(1, 1)
      assert_nil(stmt.execute.each.first)

      stmt.bind(1, 2_147_483_647)
      assert_equal(expected_row, stmt.execute.each.first)
    end

    def test_bind_with_int64
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_bigint = $1')

      stmt.bind(1, 1)
      assert_nil(stmt.execute.each.first)

      stmt.bind(1, 9_223_372_036_854_775_807)
      assert_equal(expected_row, stmt.execute.each.first)
    end

    def test_bind_with_float
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_real = $1')

      stmt.bind(1, 12_345.375)
      assert_equal(expected_row, stmt.execute.each.first)

      stmt.bind(1, 12_345.376)
      assert_nil(stmt.execute.each.first)
    end

    def test_bind_with_double
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_double = $1')

      stmt.bind(1, 12_345.6789)
      assert_equal(expected_row, stmt.execute.each.first)

      stmt.bind(1, 12_345.6788)
      assert_nil(stmt.execute.each.first)
    end

    def test_bind_with_varchar
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_varchar = $1')

      stmt.bind(1, 'str')
      assert_equal(1, stmt.execute.each.first[0])

      # block SQL injection using bind
      stmt.bind(1, "' or 1 = 1 --")
      assert_nil(stmt.execute.each.first)
    end

    def test_bind_with_time
      now = Time.now
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_timestamp = $1')

      stmt.bind(1, Time.mktime(2019, 11, 9, 12, 34, 56, 0))
      assert_equal(expected_row, stmt.execute.each.first)

      stmt.bind(1, Time.mktime(2019, 11, 9, 12, 34, 56, 123_456))
      assert_nil(stmt.execute.each.first)

      @con.query("UPDATE a SET col_timestamp = '#{now.strftime('%Y/%m/%d %H:%M:%S.%N')}'")
      stmt.bind(1, now)
      assert_equal(1, stmt.execute.each.first.first)

      stmt.bind(1, now.strftime('%Y/%m/%d %H:%M:%S') + ".#{now.nsec + 1_000_000}")
      assert_nil(stmt.execute.each.first)
    end

    def test_bind_with_date
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_date = $1')
      date = PreparedStatementTest.today

      stmt.bind(1, date)
      assert_equal(expected_row, stmt.execute.each.first)

      stmt.bind(1, date + 1)
      assert_nil(stmt.execute.each.first)
    end

    def test_bind_with_blob
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO a(id, col_blob) VALUES (NULL, $1)')
      stmt.bind(1, DuckDB::Blob.new("\0\1\2\3\4\5"))
      assert_instance_of(DuckDB::Result, stmt.execute)
      result = @con.execute('SELECT col_blob FROM a WHERE id IS NULL')
      assert_equal("\0\1\2\3\4\5".encode(Encoding::BINARY), result.first.first)

      stmt.bind(1, "\0\1\2\3\4\5".encode(Encoding::BINARY))
      assert_instance_of(DuckDB::Result, stmt.execute)
      result = @con.execute('SELECT col_blob FROM a WHERE id IS NULL')
      assert_equal("\0\1\2\3\4\5".encode(Encoding::BINARY), result.first.first)
    end

    def test_bind_with_null
      stmt = DuckDB::PreparedStatement.new(@con, 'INSERT INTO a(id) VALUES ($1)')

      stmt.bind(1, nil)
      stmt.execute
      r = @con.query('SELECT * FROM a WHERE id IS NULL')
      assert_nil(r.each.first.first)
    end

    def test_bind_with_unsupported_type
      stmt = DuckDB::PreparedStatement.new(@con, 'SELECT * FROM a WHERE col_integer = $1')

      e = assert_raises(DuckDB::Error) { stmt.bind(1, [123]) }
      assert_equal('not supported type `[123]` (Array)', e.message)
    end
  end
end
