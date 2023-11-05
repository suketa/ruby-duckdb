require 'duckdb'

DuckDB::Database.open do |db|
  db.connect do |con|
    con.query('SET threads=1')
    con.query('CREATE TABLE tbl as SELECT range a, mod(range, 10) b FROM range(10000)')
    con.query('CREATE TABLE tbl2 as SELECT range a, FROM range(10000)')
    con.query('SET ENABLE_PROGRESS_BAR=true')
    con.query('SET ENABLE_PROGRESS_BAR_PRINT=false')
    pending_result = con.async_query('SELECT count(*) FROM tbl where a = (SELECT min(a) FROM tbl2)')

    pending_result.execute_task while con.query_progress.zero?
    p con.query_progress
    con.interrupt
    while pending_result.state == :not_ready
      pending_result.execute_task
      p pending_result.state
    end
  end
end
