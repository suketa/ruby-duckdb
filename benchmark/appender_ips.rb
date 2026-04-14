# frozen_string_literal: true

# Benchmark: append_data_chunk vs row-based append methods
#
# Compares four approaches for bulk-inserting ROWS rows into DuckDB:
#
#   1. append_row       - generic dispatch (append) + end_row per row
#   2. append_typed     - typed methods (append_int32 / append_varchar) + end_row per row
#   3. append_chunk     - DataChunk#set_value (high-level) + append_data_chunk per chunk
#   4. append_chunk_raw - direct MemoryHelper writes + assign_string_element (low-level)
#                         + append_data_chunk per chunk
#
# Run: ruby -Ilib benchmark/appender_ips.rb

$LOAD_PATH.unshift File.expand_path('../lib', __dir__)
require 'duckdb'
require 'benchmark/ips'

ROWS       = 10_000
CHUNK_SIZE = DuckDB.vector_size  # 2048

# Pre-build data so data generation is not part of the measured work.
IDS   = Array.new(ROWS) { |i| i }
NAMES = Array.new(ROWS) { |i| "name_#{i}" }

TYPES = [DuckDB::LogicalType::INTEGER, DuckDB::LogicalType::VARCHAR].freeze

def new_table(con)
  con.query('DROP TABLE IF EXISTS t')
  con.query('CREATE TABLE t (id INTEGER, name VARCHAR)')
end

def with_appender
  db  = DuckDB::Database.open
  con = db.connect
  new_table(con)
  app = con.appender('t')
  yield app
  app.flush
ensure
  con&.close
  db&.close
end

puts "Ruby #{RUBY_VERSION} / DuckDB #{DuckDB.library_version}"
puts "#{ROWS} rows, vector_size=#{CHUNK_SIZE}\n\n"

Benchmark.ips do |x|
  x.report('append_row') do
    with_appender do |app|
      ROWS.times do |i|
        app.append_row(IDS[i], NAMES[i])
      end
    end
  end

  x.report('append_typed') do
    with_appender do |app|
      ROWS.times do |i|
        app.append_int32(IDS[i])
           .append_varchar(NAMES[i])
           .end_row
      end
    end
  end

  x.report('append_chunk (set_value)') do
    with_appender do |app|
      chunk  = DuckDB::DataChunk.new(TYPES)
      offset = 0
      while offset < ROWS
        len = [CHUNK_SIZE, ROWS - offset].min
        len.times do |r|
          chunk.set_value(0, r, IDS[offset + r])
          chunk.set_value(1, r, NAMES[offset + r])
        end
        chunk.size = len
        app.append_data_chunk(chunk)
        chunk.reset
        offset += len
      end
    end
  end

  x.report('append_chunk (raw)') do
    with_appender do |app|
      chunk  = DuckDB::DataChunk.new(TYPES)
      vec_id = chunk.get_vector(0)
      vec_nm = chunk.get_vector(1)
      offset = 0
      while offset < ROWS
        len    = [CHUNK_SIZE, ROWS - offset].min
        ptr_id = vec_id.get_data
        len.times do |r|
          DuckDB::MemoryHelper.write_integer(ptr_id, r, IDS[offset + r])
          vec_nm.assign_string_element(r, NAMES[offset + r])
        end
        chunk.size = len
        app.append_data_chunk(chunk)
        chunk.reset
        offset += len
      end
    end
  end

  x.compare!
end

__END__

## Results (Ruby 4.0.2 +YJIT +PRISM / DuckDB v1.5.2, 10_000 rows, vector_size=2048)
# run: ruby -Ilib benchmark/appender_ips.rb
#
# Calculating -------------------------------------
#           append_row    202.604 (± 0.5%) i/s    (4.94 ms/i)
#         append_typed    245.136 (± 1.6%) i/s    (4.08 ms/i)
# append_chunk (set_value)
#                         201.766 (± 1.5%) i/s    (4.96 ms/i)
#   append_chunk (raw)    231.878 (± 0.4%) i/s    (4.31 ms/i)
#
# Comparison:
#         append_typed:      245.1 i/s
#   append_chunk (raw):      231.9 i/s - 1.06x  slower
#           append_row:      202.6 i/s - 1.21x  slower
# append_chunk (set_value):      201.8 i/s - 1.21x  slower
#
# Effect of DataChunk#reset
# -------------------------
# Before this change, chunk-based strategies allocated a fresh DataChunk per
# chunk iteration and were the *slowest* options. Compare ratios against
# append_typed (same-run baseline) to isolate the effect of the chunk reuse,
# since absolute numbers fluctuate with Ruby/YJIT/DuckDB versions:
#
#   Strategy                 Before (vs typed)   After (vs typed)
#   -----------------------  ------------------  ----------------
#   append_chunk (raw)         1.79x slower        1.06x slower
#   append_chunk (set_value)   1.43x slower        1.21x slower
#   append_row                 1.13x slower        1.21x slower   (no change from reset)
#
# The "Before" ratios come from an earlier run on Ruby 4.0.2 / DuckDB v1.5.1
# that is preserved in git history. Only the two append_chunk paths are
# touched by DataChunk#reset; append_row and append_typed do not use
# DataChunk at all, so their ratio shifts are environmental noise, not a
# result of this change.
#
# What this benchmark shows about reset
# -------------------------------------
# Reusing one DataChunk across all append_data_chunk calls removes the
# per-chunk allocation cost. The raw path benefits most (1.79x -> 1.06x vs
# typed): it was dominated by allocation overhead. set_value improves less
# dramatically because its per-cell case dispatch still adds Ruby overhead.
# The remaining gap to append_typed on the raw path is the per-iteration
# vec_id.get_data() call — mandated by the duckdb_data_chunk_reset C API
# contract, which invalidates previously returned data pointers.

