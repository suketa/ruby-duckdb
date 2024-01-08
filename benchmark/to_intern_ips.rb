# frozen_string_literal: true

require 'bundler/setup'
require 'duckdb'
require 'benchmark/ips'

DuckDB::Result.use_chunk_each = true
db = DuckDB::Database.open
con = db.connect
con.query(<<~SQL
  CREATE TABLE t (
    date_value DATE,
    time_value TIME,
    timestamp_value TIMESTAMP,
    interval_value INTERVAL,
    hugeint_value HUGEINT,
    uuid_value UUID,
    decimal_value DECIMAL(4, 2)
  )
SQL
)
con.query(<<~SQL
  INSERT INTO t VALUES
    (
      '2019-01-01',
      '12:00:00',
      '2019-01-01 12:00:00',
      '1 day',
      12345678901234567890,
      'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
      0.12
    ),
    (
      '2019-01-01',
      '12:00:00',
      '2019-01-01 12:00:00',
      '1 day',
      12345678901234567890,
      'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
      0.12
    ),
    (
      '2019-01-01',
      '12:00:00',
      '2019-01-01 12:00:00',
      '1 day',
      12345678901234567890,
      'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
      2.12
    )
SQL
)
result = con.query('SELECT * FROM t')

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
