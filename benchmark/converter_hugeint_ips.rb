# frozen_string_literal: true

require 'bundler/setup'
require 'duckdb'
require 'benchmark/ips'

Benchmark.ips do |x|
  x.report('hugeint_convert') { DuckDB::Converter._to_hugeint_from_vector(123_456_789, 123_456_789) }
end

__END__

## before
```
✦ ❯ ruby benchmark/converter_hugeint_ips.rb
Warming up --------------------------------------
       hugeint_convert   318.524k i/100ms
Calculating -------------------------------------
       hugeint_convert      3.940M (± 0.7%) i/s -     19.748M in   5.012440s
```

## after (use bit shift)
✦ ❯ ruby benchmark/converter_hugeint_ips.rb
Warming up --------------------------------------
       hugeint_convert   347.419k i/100ms
Calculating -------------------------------------
       hugeint_convert      4.171M (± 0.3%) i/s -     21.193M in   5.081131s
