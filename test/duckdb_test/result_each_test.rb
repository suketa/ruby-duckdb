# frozen_string_literal: true

require 'test_helper'
require 'securerandom'
require 'time'

module DuckDBTest
  class ResultChunkEach < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
    end

    def teardown
      @db.close
    end

    UUID = SecureRandom.uuid
    ENUM_SQL = "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');"
    LOAD_ICU = 'LOAD icu;'
    INSTALL_ICU = 'INSTALL icu;'
    long_bits = '11111111111111111111111111111111111110101010101010101010101010101010101010101011100000000'
    timetz_expected = Time.parse(Time.now.strftime('%Y-%m-%d 12:34:56.123456+04:30'))

    EXPECTED_DECIMAL_VALUE1 = if ::DuckDBTest.duckdb_library_version >= Gem::Version.new('1.1.0')
                                BigDecimal('1.2345679')
                              else
                                BigDecimal('1.23456789')
                              end

    EXPECTED_DECIMAL_VALUE2 = if ::DuckDBTest.duckdb_library_version >= Gem::Version.new('1.1.0')
                                BigDecimal('0.00123457')
                              else
                                BigDecimal('0.00123456')
                              end

    TEST_TABLES = [
      #      DB Type  ,     DB declartion                  String Rep                                  Ruby Type             Ruby Value
      [:ok, 'BOOLEAN',      'BOOLEAN',                     'true',                                     TrueClass,            true                                                ],
      [:ok, 'TINYINT',      'TINYINT',                     1,                                          Integer,              1                                                   ],
      [:ok, 'TINYINT',      'TINYINT',                     127,                                        Integer,              127                                                 ],
      [:ok, 'TINYINT',      'TINYINT',                     -128,                                       Integer,              -128                                                ],
      [:ok, 'SMALLINT',     'SMALLINT',                    32767,                                      Integer,              32_767                                              ],
      [:ok, 'SMALLINT',     'SMALLINT',                    -32768,                                     Integer,             -32_768                                              ],
      [:ok, 'INTEGER',      'INTEGER',                     2147483647,                                 Integer,              2_147_483_647                                       ],
      [:ok, 'INTEGER',      'INTEGER',                     -2147483648,                                Integer,             -2_147_483_648                                       ],
      [:ok, 'BIGINT',       'BIGINT',                      9223372036854775807,                        Integer,              9_223_372_036_854_775_807                           ],
      [:ok, 'BIGINT',       'BIGINT',                      -9223372036854775808,                       Integer,             -9_223_372_036_854_775_808                           ],
      [:ok, 'UTINYINT',     'UTINYINT',                    255,                                        Integer,              255                                                 ],
      [:ok, 'USMALLINT',    'USMALLINT',                   65535,                                      Integer,              65_535                                              ],
      [:ok, 'UINTEGER',     'UINTEGER',                    4294967295,                                 Integer,              4_294_967_295                                       ],
      [:ok, 'UBIGINT',      'UBIGINT',                     18446744073709551615,                       Integer,              18_446_744_073_709_551_615                          ],
      [:ok, 'FLOAT',        'FLOAT',                       12345.375,                                  Float,                12_345.375                                          ],
      [:ok, 'DOUBLE',       'DOUBLE',                      123.456789,                                 Float,                123.456789                                          ],
      [:ok, 'TIMESTAMP',    'TIMESTAMP',                   "'2019-11-03 12:34:56.000001'",             Time,                 Time.local(2019, 11, 3, 12, 34, 56, 1)              ],
      [:ok, 'TIMESTAMP',    'TIMESTAMP',                   "'2019-11-03 12:34:56.00001'",              Time,                 Time.local(2019, 11, 3, 12, 34, 56, 10)             ],
      [:ok, 'TIMESTAMP',    'TIMESTAMP',                   "'infinity'",                               String,               'infinity'                                          ],
      [:ok, 'TIMESTAMP',    'TIMESTAMP',                   "'-infinity'",                              String,               '-infinity'                                         ],
      [:ok, 'DATE',         'DATE',                        "'2019-11-03'",                             Date,                 Date.new(2019,11,3)                                 ],
      [:ng, 'DATE',         'DATE',                        "'infinity'",                               String,               'infinity'                                          ],
      [:ng, 'DATE',         'DATE',                        "'-infinity'",                              String,               '-infinity'                                         ],
      [:ok, 'TIME',         'TIME',                        "'12:34:56.000001'",                        Time,                 Time.parse('12:34:56.000001')                       ],
      [:ok, 'TIME',         'TIME',                        "'12:34:56.00001'",                         Time,                 Time.parse('12:34:56.00001')                        ],
      [:ok, 'INTERVAL',     'INTERVAL',                    "'2 days ago'",                             DuckDB::Interval,     DuckDB::Interval.new(interval_days: -2)             ],
      [:ok, 'HUGEINT',      'HUGEINT',                     170141183460469231731687303715884105727,    Integer,              170_141_183_460_469_231_731_687_303_715_884_105_727 ],
      [:ok, 'HUGEINT',      'HUGEINT',                     -170141183460469231731687303715884105727,   Integer,             -170_141_183_460_469_231_731_687_303_715_884_105_727 ],
      [:ok, 'UHUGEINT',     'UHUGEINT',                    340282366920938463463374607431768211455,    Integer,              340_282_366_920_938_463_463_374_607_431_768_211_455 ],
      [:ok, 'VARCHAR',      'VARCHAR',                     "'hello'",                                  String,               'hello'                                             ],
      [:ok, 'VARCHAR',      'VARCHAR',                     "'𝘶ñîҫȫ𝘥ẹ 𝖘ţ𝗋ⅰɲ𝓰 😃'",                      String,               '𝘶ñîҫȫ𝘥ẹ 𝖘ţ𝗋ⅰɲ𝓰 😃'                                 ],
      [:ok, 'BLOB',         'BLOB',                        DuckDB::Blob.new('\0\1\2'),                 String,               '\0\1\2'.encode('ASCII-8BIT')                       ],
      [:ok, 'BLOB',         'BLOB',                        '\0\1\2'.encode('ASCII-8BIT'),              String,               '\0\1\2'.encode('ASCII-8BIT')                       ],
      [:ok, 'BLOB',         'BLOB',                        'blob',                                     String,               'blob'.encode('ASCII-8BIT')                         ],
      [:ok, 'DECIMAL',      'DECIMAL(38, 8)',              0,                                          BigDecimal,           BigDecimal('0')                                     ],
      [:ok, 'DECIMAL',      'DECIMAL(38, 8)',              1.23456789,                                 BigDecimal,           BigDecimal('1.23456789')                            ],
      [:ok, 'DECIMAL',      'DECIMAL(38, 8)',              -1.23456789,                                BigDecimal,           BigDecimal('-1.23456789')                           ],
      [:ok, 'DECIMAL',      'DECIMAL(38, 8)',              0.00000001,                                 BigDecimal,           BigDecimal('0.00000001')                            ],
      [:ok, 'DECIMAL',      'DECIMAL(38, 8)',              0.00000123,                                 BigDecimal,           BigDecimal('0.00000123')                            ],
      [:ok, 'DECIMAL',      'DECIMAL(38, 8)',              0.1,                                        BigDecimal,           BigDecimal('0.1')                                   ],
      [:ok, 'DECIMAL',      'DECIMAL(38, 8)',              1,                                          BigDecimal,           BigDecimal('1')                                     ],
      [:ok, 'DECIMAL',      'DECIMAL(2, 1)',               1,                                          BigDecimal,           BigDecimal('1')                                     ],
      [:ok, 'DECIMAL',      'DECIMAL(8, 1)',               1,                                          BigDecimal,           BigDecimal('1')                                     ],
      [:ok, 'DECIMAL',      'DECIMAL(16, 1)',              1,                                          BigDecimal,           BigDecimal('1')                                     ],
      [:ok, 'DECIMAL',      'DECIMAL(16, 1)',              -123456789,                                 BigDecimal,           BigDecimal('-123456789')                            ],
      [:ok, 'DECIMAL',      'DECIMAL(16, 0)',              123456789,                                  BigDecimal,           BigDecimal('123456789')                             ],
      [:ok, 'DECIMAL',      'DECIMAL(2, 0)',               1,                                          BigDecimal,           BigDecimal('1')                                     ],
      [:ok, 'DECIMAL',      'DECIMAL(38, 8)',              '2345678901234567890123.45678901',          BigDecimal,           BigDecimal('2345678901234567890123.45678901')       ],
      [:ok, 'DECIMAL',      'DECIMAL(38, 8)',              '-2345678901234567890123.45678901',         BigDecimal,           BigDecimal('-2345678901234567890123.45678901')      ],
      [:ok, 'DECIMAL',      'DECIMAL(38, 8)',              '1.23456789',                               BigDecimal,           BigDecimal('1.23456789')                            ],
      [:ok, 'DECIMAL',      'DECIMAL(38, 8)',              '1.234567894',                              BigDecimal,           BigDecimal('1.23456789')                            ],
      [:ok, 'DECIMAL',      'DECIMAL(38, 8)',              '1.234567895',                              BigDecimal,           EXPECTED_DECIMAL_VALUE1                             ],
      [:ok, 'DECIMAL',      'DECIMAL(38, 8)',              '0.00123456489',                            BigDecimal,           BigDecimal('0.00123456')                            ],
      [:ok, 'DECIMAL',      'DECIMAL(38, 8)',              '0.00123456589',                            BigDecimal,           EXPECTED_DECIMAL_VALUE2                             ],
      [:ok, 'TIMESTAMP_S',  'TIMESTAMP_S',                 "'2019-11-03 12:34:56.123456789'",          Time,                 Time.local(2019, 11, 3, 12, 34, 56)                 ],
      [:ok, 'TIMESTAMP_MS', 'TIMESTAMP_MS',                "'2019-11-03 12:34:56.123456789'",          Time,                 Time.parse('2019-11-3 12:34:56.123')                ],
      [:ok, 'TIMESTAMP_NS', 'TIMESTAMP_NS',                "'2019-11-03 12:34:56.123456789'",          Time,                 Time.parse('2019-11-3 12:34:56.123456')             ],
      [:ok, 'ENUM',         'mood',                        "'happy'",                                  String,               'happy'                                             ],
      [:ok, 'LIST',         'INTEGER[]',                   '[1, 2]',                                   Array,                [1, 2]                                              ],
      [:ok, 'LIST',         'INTEGER[][]',                 '[[1, 2], [3, 4]]',                         Array,                [[1, 2], [3, 4]]                                    ],
      [:ok, 'STRUCT',       'STRUCT(a INTEGER, b INTEGER)', "{'a': 1, 'b': 2}",                        Hash,                 {a: 1, b: 2 }                                       ],
      [:ok, 'MAP',          'MAP(INTEGER, INTEGER)',       'map {1: 2, 3: 4}',                         Hash,                 {1 => 2, 3 => 4}                                    ],
      [:ok, 'ARRAY',        'INTEGER[2]',                  'array_value(1::INTEGER, 2::INTEGER)',      Array,                [1, 2]                                              ],
      [:ok, 'ARRAY',        'VARCHAR[2]',                  "array_value('a', '𝘶ñîҫȫ𝘥ẹ 𝖘ţ𝗋ⅰɲ𝓰 😃')",    Array,                ['a', '𝘶ñîҫȫ𝘥ẹ 𝖘ţ𝗋ⅰɲ𝓰 😃']                          ],
      [:ok, 'UUID',         'UUID',                        "'#{UUID}'",                                String,               UUID                                                ],
      [:ok, 'UNION',        'UNION(i INTEGER, s VARCHAR)',  1,                                         Integer,              1                                                   ],
      [:ok, 'UNION',        'UNION(i INTEGER, s VARCHAR)',  "'happy'",                                 String,               'happy'                                             ],
      [:ok, 'BIT',          'BIT',                          "'010110'::BIT",                           String,               '010110'                                            ],
      [:ok, 'BIT',          'BIT',                          "'010110111'::BIT",                        String,               '010110111'                                         ],
      [:ok, 'BIT',          'BIT',                          "'#{long_bits}'::BIT",                     String,               long_bits                                           ],
      # set TIMEZONE to Asia/Kabul to test TIMETZ and TIMESTAMPTZ
      [:ok, 'TIMETZ',       'TIMETZ',                       "'2019-11-03 12:34:56.123456789'",         Time,                 timetz_expected                                     ],
      [:ok, 'TIMESTAMPTZ',  'TIMESTAMPTZ',                  "'2019-11-03 12:34:56.123456789'",         Time,                 Time.parse('2019-11-03 08:04:56.123456+0000')       ],
    ].freeze

    def prepare_test_table_and_data(db_declaration, db_type, string_rep)
      prepare_timezone('Asia/Kabul') if %w[TIMETZ TIMESTAMPTZ].include?(db_type)
      @con.query(ENUM_SQL) if db_type == 'ENUM'
      @con.query("CREATE TABLE tests (col #{db_declaration})")
      if %w[BLOB UHUGEINT].include?(db_type)
        @con.query('INSERT INTO tests VALUES ( ? )', string_rep)
      else
        @con.query("INSERT INTO tests VALUES ( #{string_rep} )")
      end
    end

    def prepare_timezone(timezone)
      @con.query(INSTALL_ICU)
      @con.query(LOAD_ICU)
      @con.query("SET TimeZone=\"#{timezone}\";")
    end

    def query_test_data
      @con.query('SELECT * FROM tests').to_a[0][0]
    end

    def async_query_test_data
      res = @con.async_query('SELECT * FROM tests')
      res.execute_task while res.state == :not_ready
      res.execute_pending.to_a[0][0]
    end

    def do_query_result_assertions(res, ruby_val, db_type, klass)
      if %w[TIME TIMETZ].include?(db_type)
        assert_equal(
          [ruby_val.hour, ruby_val.min, ruby_val.sec, ruby_val.usec, ruby_val.utc_offset],
          [res.hour, res.min, res.sec, res.usec, res.utc_offset]
        )
      else
        assert_equal(ruby_val, res)
      end
      assert_equal(klass, res.class)
    end

    TEST_TABLES.each_with_index do |spec, i|
      do_test, db_type, db_declaration, string_rep, klass, ruby_val = *spec
      define_method :"test_#{db_type}_type#{i}" do
        skip spec.to_s if do_test == :ng

        prepare_test_table_and_data(db_declaration, db_type, string_rep)

        res = query_test_data

        do_query_result_assertions(res, ruby_val, db_type, klass)
      end

      define_method :"test_stream_#{db_type}_type#{i}" do
        skip spec.to_s if do_test == :ng

        prepare_test_table_and_data(db_declaration, db_type, string_rep)

        res = async_query_test_data

        do_query_result_assertions(res, ruby_val, db_type, klass)
      end
    end

    def prepare_test_table_and_data_for_exception
      @con.query('CREATE TABLE tests (col INTEGER)')
      @con.query('INSERT INTO tests VALUES (1), (2), (3)')
    end

    # check that error is not raised when raising an error in the each block.
    def test_query_with_exception
      prepare_test_table_and_data_for_exception

      r = query_test_data
      assert_raises(StandardError) do
        r.each do |row|
          raise 'error' if row.first == 2
        end
      end
      assert(true, 'error raised')
    end

    # check that error is not raised when raising an error in the chunk_stream block.
    def test_async_query_with_exception
      prepare_test_table_and_data_for_exception

      r = async_query_test_data
      assert_raises(StandardError) do
        r.each do |row|
          raise 'error' if row.first == 2
        end
      end
      assert(true, 'error raised')
    end
  end
end
