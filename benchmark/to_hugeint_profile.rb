# frozen_string_literal: true

require 'bundler/setup'
require 'duckdb'
require 'stackprof'

db = DuckDB::Database.open
con = db.connect
con.query('CREATE TABLE hugeints (hugeint_value HUGEINT)')
con.query('INSERT INTO hugeints VALUES (123456789012345678901234567890123456789)')
result = con.query('SELECT hugeint_value FROM hugeints')

def profile(name, &block)
  profile = StackProf.run(mode: :wall, interval: 1_000) do
    2_000_000.times(&block)
  end

  result = StackProf::Report.new(profile)
  puts
  puts "=== #{name} ==="
  result.print_text
  puts
end

profile(:_to_hugeint) { result.send(:_to_hugeint, 0, 0) }
profile(:_to_hugeint_internal) { result.send(:_to_hugeint_internal, 0, 0) }
