# frozen_string_literal: true

# GH-1136: per-worker proxy threads for scalar UDF callbacks (DuckDB >= 1.5.0).
#
# Each DuckDB worker thread gets its own dedicated Ruby proxy thread, so UDF
# callbacks from different workers run concurrently instead of serializing on
# the single global executor. Two observables demonstrate it:
#   - callbacks run on multiple distinct Ruby threads (one proxy per worker;
#     with the global executor the count can never exceed two)
#   - wall-clock improves for a GVL-releasing (I/O-bound) callback
#
# Note: pure-CPU Ruby callbacks stay effectively serialized by the GVL; the
# throughput win is specific to callbacks that release it (e.g. on I/O,
# simulated here with sleep). A large base table is used so the morsel-driven
# scan actually distributes work across workers.
require 'duckdb'

ROWS = 500_000
SLEEP_EVERY = 1_000 # simulate I/O on every Nth value
SLEEP_SEC = 0.002

def register_slow_triple(con, threads_seen)
  sf = DuckDB::ScalarFunction.new
  sf.name = 'slow_triple'
  sf.add_parameter(DuckDB::LogicalType::INTEGER)
  sf.return_type = DuckDB::LogicalType::BIGINT
  sf.set_function do |v|
    threads_seen[Thread.current] = true
    sleep(SLEEP_SEC) if (v % SLEEP_EVERY).zero?
    v * 3
  end
  con.register_scalar_function(sf)
end

def timed_sum(con)
  started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  sum = con.execute('SELECT SUM(slow_triple(value)) FROM t').first.first
  [Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, sum]
end

def measure(threads)
  db = DuckDB::Database.open
  con = db.connect
  con.execute("SET threads=#{threads}")
  con.execute("CREATE TABLE t AS SELECT range::INTEGER AS value FROM range(#{ROWS})")
  threads_seen = {}
  register_slow_triple(con, threads_seen)
  elapsed, sum = timed_sum(con)
  con.close
  db.close
  [elapsed, threads_seen.size, sum]
end

expected = (ROWS - 1) * ROWS / 2 * 3
[1, 4].each do |threads|
  elapsed, distinct, sum = measure(threads)
  raise "wrong result: #{sum} (expected #{expected})" unless sum == expected

  puts "SET threads=#{threads}: #{elapsed.round(3)}s, callbacks ran on #{distinct} distinct Ruby thread(s)"
end
