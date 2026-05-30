# frozen_string_literal: true

# GH-1136: per-worker proxy threads for scalar and table UDF callbacks
# (DuckDB >= 1.5.0).
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
# simulated here with sleep). The scalar section uses a large base table so
# the morsel-driven scan actually distributes work across workers; the table
# section sets both planner hints (set_cardinality + max_threads) for the
# same reason.
require 'duckdb'

# --- scalar UDF -------------------------------------------------------------

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

def timed_sum(con, query)
  started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  sum = con.execute(query).first.first
  [Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, sum]
end

def measure_scalar(threads)
  db = DuckDB::Database.open
  con = db.connect
  con.execute("SET threads=#{threads}")
  con.execute("CREATE TABLE t AS SELECT range::INTEGER AS value FROM range(#{ROWS})")
  threads_seen = {}
  register_slow_triple(con, threads_seen)
  elapsed, sum = timed_sum(con, 'SELECT SUM(slow_triple(value)) FROM t')
  con.close
  db.close
  [elapsed, threads_seen.size, sum]
end

# --- table UDF --------------------------------------------------------------

CHUNKS = 200
ROWS_PER_CHUNK = 50
CHUNK_SLEEP_SEC = 0.005 # simulate I/O per emitted chunk

def emit_chunk(output)
  ROWS_PER_CHUNK.times { |i| output.set_value(0, i, 1) }
  output.size = ROWS_PER_CHUNK
  sleep(CHUNK_SLEEP_SEC)
end

def emitter_bind_block
  proc do |bind_info|
    bind_info.add_result_column('v', DuckDB::LogicalType::BIGINT)
    # Tell the planner there is real work so it distributes across workers.
    bind_info.set_cardinality(CHUNKS * ROWS_PER_CHUNK, false)
  end
end

def emitter_execute_block(threads_seen)
  remaining = CHUNKS
  mutex = Mutex.new
  proc do |_info, output|
    threads_seen[Thread.current] = true
    has_work = mutex.synchronize { (remaining -= 1) >= 0 }
    has_work ? emit_chunk(output) : output.size = 0
  end
end

def register_slow_emitter(con, threads_seen)
  tf = DuckDB::TableFunction.new
  tf.name = 'slow_emitter'
  tf.bind(&emitter_bind_block)
  # Without max_threads DuckDB assigns a single worker and the proxy never fires.
  tf.init { |init_info| init_info.max_threads = 4 }
  tf.execute(&emitter_execute_block(threads_seen))
  con.register_table_function(tf)
end

def measure_table(threads)
  db = DuckDB::Database.open
  con = db.connect
  con.execute("SET threads=#{threads}")
  threads_seen = {}
  register_slow_emitter(con, threads_seen)
  elapsed, sum = timed_sum(con, 'SELECT SUM(v) FROM slow_emitter()')
  con.close
  db.close
  [elapsed, threads_seen.size, sum]
end

# --- run both ---------------------------------------------------------------

def report(label, expected)
  puts "#{label}:"
  [1, 4].each do |threads|
    elapsed, distinct, sum = yield(threads)
    raise "wrong result: #{sum} (expected #{expected})" unless sum == expected

    puts "  SET threads=#{threads}: #{elapsed.round(3)}s, callbacks ran on #{distinct} distinct Ruby thread(s)"
  end
end

report('scalar UDF (slow_triple)', (ROWS - 1) * ROWS / 2 * 3) { |threads| measure_scalar(threads) }
report('table UDF (slow_emitter)', CHUNKS * ROWS_PER_CHUNK) { |threads| measure_table(threads) }
