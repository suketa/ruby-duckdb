# frozen_string_literal: true

# Import a Polars DataFrame into a DuckDB table via the Arrow C stream
# protocol (experimental).
#
# DuckDB::Connection#append_arrow reads any Arrow producer (an object
# responding to #arrow_c_stream, such as a Polars::DataFrame) and appends its
# chunks into an existing DuckDB table in columnar form — no per-row Ruby
# object conversion. Once imported, the data can be queried with SQL.
#
#   gem install polars-df

require 'duckdb'
require 'polars-df'

df = Polars::DataFrame.new(
  {
    'id' => [1, 2, 3],
    'name' => %w[Alice Bob Cathy],
    'score' => [89.5, 72.3, 95.1]
  }
)

db = DuckDB::Database.open
con = db.connect

# The target table must already exist; DuckDB casts compatible column types.
con.query('CREATE TABLE people (id BIGINT, name VARCHAR, score DOUBLE)')

rows = con.append_arrow('people', df)
puts "appended #{rows} rows"
# => appended 3 rows

result = con.query('SELECT name, score FROM people WHERE score > 80 ORDER BY score DESC').to_a
p result
# => [["Cathy", 95.1], ["Alice", 89.5]]

con.close
db.close
