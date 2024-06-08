# frozen_string_literal: true

require 'test_helper'
require 'securerandom'

if DuckDB::Result.instance_methods.include?(:chunk_each)
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
      TEST_TABLES = [
        #      DB Type  ,  DB declartion                  String Rep                                  Ruby Type             Ruby Value
        [:ok, 'BOOLEAN',   'BOOLEAN',                     'true',                                     TrueClass,            true                                                ],
        [:ok, 'TINYINT',   'TINYINT',                     1,                                          Integer,              1                                                   ],
        [:ok, 'TINYINT',   'TINYINT',                     127,                                        Integer,              127                                                 ],
        [:ok, 'TINYINT',   'TINYINT',                     -128,                                       Integer,              -128                                                ],
        [:ok, 'SMALLINT',  'SMALLINT',                    32767,                                      Integer,              32_767                                              ],
        [:ok, 'SMALLINT',  'SMALLINT',                    -32768,                                     Integer,             -32_768                                              ],
        [:ok, 'INTEGER',   'INTEGER',                     2147483647,                                 Integer,              2_147_483_647                                       ],
        [:ok, 'INTEGER',   'INTEGER',                     -2147483648,                                Integer,             -2_147_483_648                                       ],
        [:ok, 'BIGINT',    'BIGINT',                      9223372036854775807,                        Integer,              9_223_372_036_854_775_807                           ],
        [:ok, 'BIGINT',    'BIGINT',                      -9223372036854775808,                       Integer,             -9_223_372_036_854_775_808                           ],
        [:ok, 'UTINYINT',  'UTINYINT',                    255,                                        Integer,              255                                                 ],
        [:ok, 'USMALLINT', 'USMALLINT',                   65535,                                      Integer,              65_535                                              ],
        [:ok, 'UINTEGER',  'UINTEGER',                    4294967295,                                 Integer,              4_294_967_295                                       ],
        [:ok, 'UBIGINT',   'UBIGINT',                     18446744073709551615,                       Integer,              18_446_744_073_709_551_615                          ],
        [:ok, 'HUGEINT',   'HUGEINT',                     170141183460469231731687303715884105727,    Integer,              170_141_183_460_469_231_731_687_303_715_884_105_727 ],
        [:ok, 'HUGEINT',   'HUGEINT',                     -170141183460469231731687303715884105727,   Integer,             -170_141_183_460_469_231_731_687_303_715_884_105_727 ],
        [:ok, 'FLOAT',     'FLOAT',                       12345.375,                                  Float,                12_345.375                                          ],
        [:ok, 'DOUBLE',    'DOUBLE',                      123.456789,                                 Float,                123.456789                                          ],
        [:ok, 'TIMESTAMP', 'TIMESTAMP',                   "'2019-11-03 12:34:56.000001'",             Time,                 Time.local(2019, 11, 3, 12, 34, 56, 1)              ],
        [:ok, 'TIMESTAMP', 'TIMESTAMP',                   "'2019-11-03 12:34:56.00001'",              Time,                 Time.local(2019, 11, 3, 12, 34, 56, 10)             ],
        [:ok, 'TIME',      'TIME',                        "'12:34:56.000001'",                        Time,                 Time.parse('12:34:56.000001')                       ],
        [:ok, 'TIME',      'TIME',                        "'12:34:56.00001'",                         Time,                 Time.parse('12:34:56.00001')                        ],
        [:ok, 'DATE',      'DATE',                        "'2019-11-03'",                             Date,                 Date.new(2019,11,3)                                 ],
        [:ok, 'INTERVAL',  'INTERVAL',                    "'2 days ago'",                             DuckDB::Interval,     DuckDB::Interval.new(interval_days: -2)             ],
        [:ok, 'VARCHAR',   'VARCHAR',                     "'hello'",                                  String,               'hello'                                             ],
        [:ok, 'VARCHAR',   'VARCHAR',                     "'𝘶ñîҫȫ𝘥ẹ 𝖘ţ𝗋ⅰɲ𝓰 😃'",                      String,               '𝘶ñîҫȫ𝘥ẹ 𝖘ţ𝗋ⅰɲ𝓰 😃'                                 ],
        [:ok, 'BLOB',      'BLOB',                        "'blob'",                                   String,               String.new('blob', encoding: 'ASCII-8BIT')          ],
        [:ok, 'DECIMAL',   'DECIMAL(38, 8)',              0,                                          BigDecimal,           BigDecimal('0')                                     ],
        [:ok, 'DECIMAL',   'DECIMAL(38, 8)',              1.23456789,                                 BigDecimal,           BigDecimal('1.23456789')                            ],
        [:ok, 'DECIMAL',   'DECIMAL(38, 8)',              -1.23456789,                                BigDecimal,           BigDecimal('-1.23456789')                           ],
        [:ok, 'DECIMAL',   'DECIMAL(38, 8)',              0.00000001,                                 BigDecimal,           BigDecimal('0.00000001')                            ],
        [:ok, 'DECIMAL',   'DECIMAL(38, 8)',              0.00000123,                                 BigDecimal,           BigDecimal('0.00000123')                            ],
        [:ok, 'DECIMAL',   'DECIMAL(38, 8)',              0.1,                                        BigDecimal,           BigDecimal('0.1')                                   ],
        [:ok, 'DECIMAL',   'DECIMAL(38, 8)',              1,                                          BigDecimal,           BigDecimal('1')                                     ],
        [:ok, 'DECIMAL',   'DECIMAL(2, 1)',               1,                                          BigDecimal,           BigDecimal('1')                                     ],
        [:ok, 'DECIMAL',   'DECIMAL(8, 1)',               1,                                          BigDecimal,           BigDecimal('1')                                     ],
        [:ok, 'DECIMAL',   'DECIMAL(16, 1)',              1,                                          BigDecimal,           BigDecimal('1')                                     ],
        [:ok, 'DECIMAL',   'DECIMAL(16, 1)',              -123456789,                                 BigDecimal,           BigDecimal('-123456789')                            ],
        [:ok, 'DECIMAL',   'DECIMAL(16, 0)',              123456789,                                  BigDecimal,           BigDecimal('123456789')                             ],
        [:ok, 'DECIMAL',   'DECIMAL(2, 0)',               1,                                          BigDecimal,           BigDecimal('1')                                     ],
        [:ok, 'DECIMAL',   'DECIMAL(38, 8)',              '2345678901234567890123.45678901',          BigDecimal,           BigDecimal('2345678901234567890123.45678901')       ],
        [:ok, 'DECIMAL',   'DECIMAL(38, 8)',              '-2345678901234567890123.45678901',         BigDecimal,           BigDecimal('-2345678901234567890123.45678901')      ],
        [:ok, 'DECIMAL',   'DECIMAL(38, 8)',              '1.23456789',                               BigDecimal,           BigDecimal('1.23456789')                            ],
        [:ok, 'DECIMAL',   'DECIMAL(38, 8)',              '1.234567898',                              BigDecimal,           BigDecimal('1.23456789')                            ],
        [:ok, 'DECIMAL',   'DECIMAL(38, 8)',              '0.00123456789',                            BigDecimal,           BigDecimal('0.00123456')                            ],
        [:ok, 'ENUM',      'mood',                        "'happy'",                                  String,               'happy'                                             ],
        [:ok, 'UUID',      'UUID',                        "'#{UUID}'",                                String,               UUID                                                ],
        # FIXME: LIST, MAP STRUCT values are always nil
        [:ng, 'LIST',      'INTEGER[]',                   '[1, 2]',                                   Array,                [1, 2]                                              ],
        [:ng, 'LIST',      'INTEGER[][]',                 '[[1, 2], [3, 4]]',                         Array,                [[1, 2], [3, 4]]                                    ],
        [:ng, 'MAP',       'MAP(INTEGER, INTEGER)',       'map {1: 2, 3: 4}',                         Hash,                 {1 => 2, 3 => 4}                                    ],
        [:ng, 'STRUCT',    'STRUCT(a INTEGER, b INTEGER)', "{'a': 1, 'b': 2}",                        Hash,                 {"a" => 1, "b" => 2 }                               ],
      ].freeze

      TEST_TABLES.each_with_index do |spec, i|
        do_test, db_type, db_declaration, string_rep, klass, ruby_val = *spec
        define_method :"test_#{db_type}_type#{i}" do
          skip if do_test == :ng
          @con.query(ENUM_SQL)
          @con.query("CREATE TABLE tests (col #{db_declaration})")
          @con.query("INSERT INTO tests VALUES ( #{string_rep} )")
          res = @con.query('SELECT * FROM tests').to_a[0][0]
          if db_type == 'TIME'
            assert_equal(
              [ruby_val.hour, ruby_val.min, ruby_val.sec, ruby_val.usec],
              [res.hour, res.min, res.sec, res.usec]
            )
          else
            assert_equal(ruby_val, res)
          end
          assert_equal(klass, res.class)
        end
      end

      TEST_TABLES.each_with_index do |spec, i|
        do_test, db_type, db_declaration, string_rep, klass, ruby_val = *spec
        define_method :"test_stream_#{db_type}_type#{i}" do
          skip if do_test == :ng
          @con.query(ENUM_SQL)
          @con.query("CREATE TABLE tests (col #{db_declaration})")
          @con.query("INSERT INTO tests VALUES ( #{string_rep} )")
          res = @con.async_query_stream('SELECT * FROM tests')
          res.execute_task while res.state == :not_ready
          res = res.execute_pending.to_a[0][0]
          if db_type == 'TIME'
            assert_equal(
              [ruby_val.hour, ruby_val.min, ruby_val.sec, ruby_val.usec],
              [res.hour, res.min, res.sec, res.usec]
            )
          else
            assert_equal(ruby_val, res)
          end
          assert_equal(klass, res.class)
        end
      end

      def test_streaming?
        r = @con.query('SELECT 1')
        assert_equal(false, r.streaming?)
      end

      def test_chunk_each_with_exception
        # check that error is not raised when raising an error in the chunk_each block.
        @con.query('CREATE TABLE tests (col INTEGER)')
        @con.query('INSERT INTO tests VALUES (1), (2), (3), (4), (5)')
        r = @con.query('SELECT * FROM tests')
        assert_raises(StandardError) do
          r.each do |row|
            raise 'error' if row.first == 3
          end
        end
        assert(true, 'error raised')
      end

      def test_chunk_stream_with_exception
        # check that error is not raised when raising an error in the chunk_stream block.
        @con.query('CREATE TABLE tests (col INTEGER)')
        @con.query('INSERT INTO tests VALUES (1), (2), (3), (4), (5)')
        r = @con.async_query_stream('SELECT * FROM tests')
        r.execute_task while r.state == :not_ready
        r = r.execute_pending
        assert_raises(StandardError) do
          r.each do |row|
            raise 'error' if row.first == 3
          end
        end
        assert(true, 'error raised')
      end
    end
  end
end
