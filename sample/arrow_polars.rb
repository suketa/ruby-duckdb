# frozen_string_literal: true

# Export a DuckDB query result to a Polars DataFrame via the Arrow C stream
# protocol (experimental).
#
# DuckDB::Result#arrow_c_stream exports the result as an Arrow C stream
# (Arrow C Data Interface). Polars::DataFrame.new accepts any object that
# responds to #arrow_c_stream, so the query result can be passed directly —
# the data moves in columnar form, without converting each row to Ruby
# objects.
#
#   gem install polars-df

require 'duckdb'
require 'polars-df'

db = DuckDB::Database.open
con = db.connect

con.query('CREATE TABLE users (id INTEGER, name VARCHAR, score DOUBLE)')
con.query(<<~SQL)
  INSERT INTO users VALUES
    (1, 'Alice', 89.5),
    (2, 'Bob', 72.3),
    (3, 'Cathy', 95.1)
SQL

result = con.query('SELECT * FROM users ORDER BY id')
df = Polars::DataFrame.new(result)

puts df
#  shape: (3, 3)
#  ┌─────┬───────┬───────┐
#  │ id  ┆ name  ┆ score │
#  │ --- ┆ ---   ┆ ---   │
#  │ i32 ┆ str   ┆ f64   │
#  ╞═════╪═══════╪═══════╡
#  │ 1   ┆ Alice ┆ 89.5  │
#  │ 2   ┆ Bob   ┆ 72.3  │
#  │ 3   ┆ Cathy ┆ 95.1  │
#  └─────┴───────┴───────┘

puts df.group_by('name').agg(Polars.col('score').mean)['name'].sort.to_a.inspect
# => ["Alice", "Bob", "Cathy"]

# The consumer takes ownership of the stream's contents, so a result can be
# exported only once.
begin
  Polars::DataFrame.new(result)
rescue DuckDB::Error => e
  puts e.message # => result is already exported as an Arrow stream
end

con.close
db.close
