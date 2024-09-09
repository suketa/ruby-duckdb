require 'bundler/setup'
require 'duckdb'
require 'benchmark/ips'

DuckDB::Database.open do |db|
  db.connect do |con|
    con.query('SET threads=1')
    con.query('CREATE TABLE tbl as SELECT range a, mod(range, 10) b FROM range(100000)')
    con.query('CREATE TABLE tbl2 as SELECT range a, mod(range, 10) b FROM range(100000)')
    query_sql = 'SELECT * FROM tbl where b = (SELECT min(b) FROM tbl2)'
    print <<~END_OF_HEAD

      Benchmark: Get first record ======================================
    END_OF_HEAD

    Benchmark.ips do |x|
      x.report('async_query') do
        pending_result = con.async_query(query_sql)

        pending_result.execute_task while pending_result.state == :not_ready
        result = pending_result.execute_pending
        result.each.first
      end
      x.report('query') do
        result = con.query(query_sql)
        result.each.first
      end
      x.report('async_query_stream') do
        pending_result = con.async_query_stream(query_sql)

        pending_result.execute_task while pending_result.state == :not_ready
        result = pending_result.execute_pending
        result.each.first
      end
    end

    print <<~END_OF_HEAD


      Benchmark: Get all records ======================================
    END_OF_HEAD

    Benchmark.ips do |x|
      x.report('async_query') do
        pending_result = con.async_query(query_sql)

        pending_result.execute_task while pending_result.state == :not_ready
        result = pending_result.execute_pending
        result.each.to_a
      end
      x.report('query') do
        result = con.query(query_sql)
        result.each.to_a
      end
      x.report('async_query_stream') do
        pending_result = con.async_query_stream(query_sql)

        pending_result.execute_task while pending_result.state == :not_ready
        result = pending_result.execute_pending
        result.each.to_a
      end
    end
  end
end

__END__

results:
Benchmark: Get first record ======================================
Warming up --------------------------------------
         async_query    70.000  i/100ms
               query    88.000  i/100ms
  async_query_stream   188.000  i/100ms
Calculating -------------------------------------
         async_query    847.191  (± 4.6%) i/s -      4.270k in   5.051650s
               query    850.509  (± 3.8%) i/s -      4.312k in   5.078167s
  async_query_stream      1.757k (± 7.3%) i/s -      8.836k in   5.057142s


Benchmark: Get all records ======================================
Warming up --------------------------------------
         async_query    40.000  i/100ms
               query    40.000  i/100ms
  async_query_stream    39.000  i/100ms
Calculating -------------------------------------
         async_query    402.567  (± 0.5%) i/s -      2.040k in   5.067639s
               query    406.632  (± 0.7%) i/s -      2.040k in   5.017079s
  async_query_stream    395.532  (± 0.8%) i/s -      1.989k in   5.028955s
