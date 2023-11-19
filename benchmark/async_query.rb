require 'bundler/setup'
require 'duckdb'
require 'benchmark/ips'


DuckDB::Result.use_chunk_each = true
DuckDB::Database.open do |db|
  db.connect do |con|
    con.query('SET threads=1')
    con.query('CREATE TABLE tbl as SELECT range a, mod(range, 10) b FROM range(100000)')
    con.query('CREATE TABLE tbl2 as SELECT range a, mod(range, 10) b FROM range(100000)')
    query_sql = 'SELECT * FROM tbl where b = (SELECT min(b) FROM tbl2)'
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
  end
end
