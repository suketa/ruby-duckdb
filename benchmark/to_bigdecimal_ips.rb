# frozen_string_literal: true

require 'bundler/setup'
require 'duckdb'
require 'benchmark/ips'

db = DuckDB::Database.open
con = db.connect
con.query('CREATE TABLE decimals (decimal_value DECIMAL(38, 3))')
con.query('INSERT INTO decimals VALUES (1234567890123.456)')
result = con.query('SELECT decimal_value FROM decimals')

Benchmark.ips do |x|
  x.report('_to_decimal') { result.send(:_to_decimal, 0, 0) }
  x.report('_to_decimal_internal') { result.send(:_to_decimal_internal, 0, 0) }
end
