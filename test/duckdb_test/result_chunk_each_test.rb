# frozen_string_literal: true

#
# require 'test_helper'
#
#   module DuckDBTest
#     class ResultChunkEach < Minitest::Test
#     test_table = <<~TABLE
#     | DB Type   | DB declartion                  | String Rep                                | Ruby Type             |Ruby Value                                          |
#     | BOOLEAN   | BOOLEAN                        | true                                      | TrueClass             |true                                                |
#     | TINYINT   | TINYINT                        | 1                                         | Integer               |1                                                   |
#     | SMALLINT  | SMALLINT                       | 32767                                     | Integer               |32_767                                              |
#     | INTEGER   | INTEGER                        | 2147483647                                | Integer               |2_147_483_647                                       |
#     | BIGINT    | BIGINT                         | 9223372036854775807                       | Integer               |9_223_372_036_854_775_807                           |
#     | HUGEINT   | HUGEINT                        | 170141183460469231731687303715884105727   | Integer               |170_141_183_460_469_231_731_687_303_715_884_105_727 |
#     | FLOAT     | FLOAT                          | 12345.375                                 | Float                 |12_345.375                                          |
#     | DOUBLE    | DOUBLE                         | 123.456789                                | Float                 |123.456789                                          |
#     | TIMESTAMP | TIMESTAMP                      | '2019-11-03 12:34:56'                     | Time                  |Time.new(2019,11,3,12,34,56)                        |
#     | DATE      | DATE                           | '2019-11-03'                              | Date                  |Date.new(2019,11,3)                                 |
#     | INTERVAL  | INTERVAL                       | '2 days ago'                              | Integer               |86400                                               |
#     | VARCHAR   | VARCHAR                        | 'hello'                                   | String                |'hello'                                             |
#     | VARCHAR   | VARCHAR                        | '𝘶ñîҫȫ𝘥ẹ 𝖘ţ𝗋ⅰɲ𝓰 😃'                       | String                |'𝘶ñîҫȫ𝘥ẹ 𝖘ţ𝗋ⅰɲ𝓰 😃'                                 |
#     | BLOB      | BLOB                           | 'blob'                                    | String                |'blob'.force_encoding('ASCII-8BIT')                 |
#     | LIST      | INTEGER[]                      | [1, 2]                                    | Array                 |[1, 2]                                              |
#     | LIST      | INTEGER[][]                    | [[1, 2], [3, 4]]                          | Array                 |[[1, 2], [3, 4]]                                    |
#     | MAP       | MAP(INTEGER, INTEGER)          | map {1: 2, 3: 4}                          | Hash                  |{1 => 2, 3 => 4}                                    |
#     | STRUCT    | STRUCT(a INTEGER, b INTEGER)   | {'a': 1, 'b': 2}                          | Hash                  |{a: 1, b: 2 }                                       |
#     TABLE
#
#     test_table.lines[1..-1].each do |spec|
#       db_type, db_declaration, string_rep, klass, ruby_val = *(spec.split('|')[1..-1].map(&:strip))
#       define_method :"test_#{db_type}_type" do
#         con = DuckDB::Database.open.connect
#         con.query("CREATE TABLE test_#{db_type} (col #{db_declaration})")
#         con.query("INSERT INTO test_#{db_type} VALUES ( #{string_rep} )")
#         res = con.query("SELECT * FROM test_#{db_type}").to_a[0][0]
#         puts res.inspect
#         assert_equal(eval(ruby_val), res)
#         assert_equal(eval(klass), res.class)
#       end
#     end
#     end
