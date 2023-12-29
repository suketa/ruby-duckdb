# frozen_string_literal: true

require 'bundler/setup'
require 'duckdb'
require 'benchmark/ips'

DuckDB::Result.use_chunk_each = true
db = DuckDB::Database.open
con = db.connect
con.query('CREATE TABLE t (value DATE)')
con.query("INSERT INTO t VALUES ('2019-01-01'), ('2021-01-01'), ('2021-01-01')")
result = con.query('SELECT value FROM t')

Benchmark.ips do |x|
  x.report('_to_date') { result.each.to_a }
end

__END__
```
## before
ruby 3.3.0 (2023-12-25 revision 5124f9ac75) [x86_64-linux]
Warming up --------------------------------------
            _to_date    30.790k i/100ms
Calculating -------------------------------------
            _to_date    365.254k (± 0.2%) i/s -      1.847M in   5.057875s

## after
ruby 3.3.0 (2023-12-25 revision 5124f9ac75) [x86_64-linux]
Warming up --------------------------------------
            _to_date    36.047k i/100ms
Calculating -------------------------------------
            _to_date    383.760k (± 3.3%) i/s -      1.947M in   5.077849s
