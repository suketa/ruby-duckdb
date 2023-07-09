# frozen_string_literal: true

require 'bundler/setup'
require 'duckdb'
require 'benchmark/ips'

db = DuckDB::Database.open
con = db.connect
con.query('CREATE TABLE hugeints (hugeint_val HUGEINT)')
con.query('INSERT INTO hugeints VALUES (1234567890123456789012345678901234)')
result = con.query('SELECT hugeint_val FROM hugeints')

Benchmark.ips do |x|
  x.report('hugeint_convert') { result.each.to_a[0][0] }
end

__END__

## before
```
✦ ❯ ruby benchmark/get_converter_module_ips.rb
Warming up --------------------------------------
     hugeint_convert    45.376k i/100ms
Calculating -------------------------------------
     hugeint_convert    552.127k (± 0.7%) i/s -      2.768M in   5.013483s
```
