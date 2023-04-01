# frozen_string_literal: true

require 'bundler/setup'
require 'duckdb'
require 'benchmark/ips'

db = DuckDB::Database.open
con = db.connect
con.query('CREATE TABLE hugeints (hugeint_value HUGEINT)')
con.query('INSERT INTO hugeints VALUES (123456789012345678901234567890123456789)')
result = con.query('SELECT hugeint_value FROM hugeints')

Benchmark.ips do |x|
  x.report('_to_hugeint') { result.send(:_to_hugeint, 0, 0) }
  x.report('_to_hugeint_internal') { result.send(:_to_hugeint_internal, 0, 0) }
end
