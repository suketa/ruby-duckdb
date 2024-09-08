require 'duckdb'

DuckDB::Database.open do |db|
  db.connect do |con|
    con.query('SET threads=1')
    con.query('CREATE TABLE tbl as SELECT range a, mod(range, 10) b FROM range(10000)')
    con.query('CREATE TABLE tbl2 as SELECT range a, mod(range, 10) b FROM range(10000)')
    # con.query('SET ENABLE_PROGRESS_BAR=true')
    # con.query('SET ENABLE_PROGRESS_BAR_PRINT=false')
    pending_result = con.async_query('SELECT * FROM tbl where b = (SELECT min(b) FROM tbl2)')

    # con.interrupt
    while pending_result.state == :not_ready
      pending_result.execute_task
      print '.'
      $stdout.flush
      sleep 0.01
    end
    result = pending_result.execute_pending
    puts
    p result.each.first
  end
end
