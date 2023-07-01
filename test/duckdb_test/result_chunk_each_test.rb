# frozen_string_literal: true

require 'test_helper'
require 'securerandom'

if DuckDB::Result.instance_methods.include?(:chunk_each)
  module DuckDBTest
    class ResultChunkEach < Minitest::Test
      def setup
        DuckDB::Result.use_chunk_each = true
        @db = DuckDB::Database.open
        @con = @db.connect
      end

      def teardown
        DuckDB::Result.use_chunk_each = false
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
        [:ok, 'INTEGER',   'INTEGER',                     2147483647,                                 Integer,              2_147_483_647                                       ],
        [:ok, 'BIGINT',    'BIGINT',                      9223372036854775807,                        Integer,              9_223_372_036_854_775_807                           ],
        [:ok, 'UTINYINT',  'UTINYINT',                    255,                                        Integer,              255                                                 ],
        [:ok, 'USMALLINT', 'USMALLINT',                   65535,                                      Integer,              65_535                                              ],
        [:ok, 'HUGEINT',   'HUGEINT',                     170141183460469231731687303715884105727,    Integer,              170_141_183_460_469_231_731_687_303_715_884_105_727 ],
        [:ok, 'FLOAT',     'FLOAT',                       12345.375,                                  Float,                12_345.375                                          ],
        [:ok, 'DOUBLE',    'DOUBLE',                      123.456789,                                 Float,                123.456789                                          ],
        [:ok, 'TIMESTAMP', 'TIMESTAMP',                   "'2019-11-03 12:34:56'",                    Time,                 Time.new(2019,11,3,12,34,56)                        ],
        [:ok, 'DATE',      'DATE',                        "'2019-11-03'",                             Date,                 Date.new(2019,11,3)                                 ],
        [:ok, 'NTERVAL',   'INTERVAL',                    "'2 days ago'",                             Hash,                 { year: 0, month: 0, day: -2, hour: 0, min: 0, sec: 0, usec: 0 } ],
        [:ok, 'VARCHAR',   'VARCHAR',                     "'hello'",                                  String,               'hello'                                             ],
        [:ok, 'VARCHAR',   'VARCHAR',                     "'𝘶ñîҫȫ𝘥ẹ 𝖘ţ𝗋ⅰɲ𝓰 😃'",                      String,               '𝘶ñîҫȫ𝘥ẹ 𝖘ţ𝗋ⅰɲ𝓰 😃'                                 ],
        [:ok, 'BLOB',      'BLOB',                        "'blob'",                                   String,               String.new('blob', encoding: 'ASCII-8BIT')          ],
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
          assert_equal(ruby_val, res)
          assert_equal(klass, res.class)
        end
      end
    end
  end
end
