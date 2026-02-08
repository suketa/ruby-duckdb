# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ConnectionTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
    end

    def test_query
      assert_instance_of(DuckDB::Result, @con.query('CREATE TABLE table1 (id INTEGER)'))
    end

    def test_query_with_valid_params
      @con.query('CREATE TABLE t (col1 INTEGER, col2 STRING)')

      assert_instance_of(DuckDB::Result, @con.query('INSERT INTO t VALUES(?, ?)', 1, 'a'))
      r = @con.query('SELECT col1, col2 FROM t WHERE col1 = ? and col2 = ?', 1, 'a')

      r = r.to_a # fix for using duckdb_fetch_chunk in Result#chunk_each

      assert_equal(1, r.each.first[0])
      assert_equal('a', r.each.first[1])
    end

    def test_query_with_valid_hash_params
      @con.query('CREATE TABLE t (col1 INTEGER, col2 STRING)')

      assert_instance_of(DuckDB::Result, @con.query('INSERT INTO t VALUES($col1, $col2)', col2: 'a', col1: 1))
      r = @con.query('SELECT col1, col2 FROM t WHERE col1 = $col1 and col2 = $col2', col2: 'a', col1: 1)

      r = r.to_a # fix for using duckdb_fetch_chunk in Result#chunk_each

      assert_equal(1, r.each.first[0])
      assert_equal('a', r.each.first[1])
    end

    def test_query_with_invalid_params
      assert_raises(DuckDB::Error) { @con.query('foo', 'bar') }
      assert_raises(ArgumentError) { @con.query }
      assert_raises(TypeError) { @con.query(1) }
    end

    def test_query_with_invalid_sql
      invalid_sql = 'CREATE TABLE table1 ('
      assert_raises(DuckDB::Error) { @con.query(invalid_sql) }
    end

    def test_async_query
      pending_result = @con.async_query('CREATE TABLE table1 (id INTEGER)')

      assert_instance_of(DuckDB::PendingResult, pending_result)
    end

    def test_async_query_with_valid_params
      @con.query('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      @con.query('INSERT INTO t VALUES(?, ?)', 1, 'a')
      pending_result = @con.async_query('SELECT col1, col2 FROM t WHERE col1 = ? and col2 = ?', 1, 'a')
      pending_result.execute_task
      sleep 0.1
      result = pending_result.execute_pending

      assert_equal([1, 'a'], result.each.first)
    end

    def test_async_query_with_valid_hash_params
      @con.query('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      @con.query('INSERT INTO t VALUES($col1, $col2)', col2: 'a', col1: 1)

      pending_result = @con.async_query(
        'SELECT col1, col2 FROM t WHERE col1 = $col1 and col2 = $col2', col2: 'a', col1: 1
      )
      pending_result.execute_task
      sleep 0.1
      result = pending_result.execute_pending

      assert_equal([1, 'a'], result.each.first)
    end

    def test_async_query_with_invalid_params
      assert_raises(DuckDB::Error) { @con.async_query('foo', 'bar') }
      assert_raises(ArgumentError) { @con.async_query }
      assert_raises(TypeError) { @con.async_query(1) }
    end

    def test_async_query_with_invalid_sql
      invalid_sql = 'CREATE TABLE table1 ('
      assert_raises(DuckDB::Error) { @con.async_query(invalid_sql) }
    end

    def test_async_query_stream
      pending_result = @con.async_query_stream('CREATE TABLE table1 (id INTEGER)')

      assert_instance_of(DuckDB::PendingResult, pending_result)
    end

    def test_async_query_stream_with_valid_params
      @con.query('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      @con.query('INSERT INTO t VALUES(?, ?)', 1, 'a')
      pending_result = @con.async_query_stream('SELECT col1, col2 FROM t WHERE col1 = ? and col2 = ?', 1, 'a')
      pending_result.execute_task
      sleep 0.1
      result = pending_result.execute_pending

      assert_equal([1, 'a'], result.each.first)
    end

    def test_async_query_stream_with_invalid_params
      assert_raises(DuckDB::Error) { @con.async_query_stream('foo', 'bar') }
      assert_raises(ArgumentError) { @con.async_query_stream }
      assert_raises(TypeError) { @con.async_query_stream(1) }
    end

    def test_async_query_stream_with_invalid_sql
      invalid_sql = 'CREATE TABLE table1 ('
      assert_raises(DuckDB::Error) { @con.async_query_stream(invalid_sql) }
    end

    def test_async_query_stream_with_valid_hash_params
      @con.query('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      @con.query('INSERT INTO t VALUES($col1, $col2)', col2: 'a', col1: 1)

      pending_result = @con.async_query_stream(
        'SELECT col1, col2 FROM t WHERE col1 = $col1 and col2 = $col2', col2: 'a', col1: 1
      )
      pending_result.execute_task
      sleep 0.1
      result = pending_result.execute_pending

      assert_equal([1, 'a'], result.each.first)
    end

    def test_execute
      @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)')

      assert_instance_of(DuckDB::Result, @con.execute('INSERT INTO t VALUES(?, ?)', 1, 'a'))
      r = @con.execute('SELECT col1, col2 FROM t WHERE col1 = ? and col2 = ?', 1, 'a')

      r = r.to_a # fix for using duckdb_fetch_chunk in Result#chunk_each

      assert_equal(1, r.each.first[0])
      assert_equal('a', r.each.first[1])
    end

    def test_query_progress_initial_state
      @con.query('SET threads=1')

      assert_equal(-1, @con.query_progress.percentage)
    end

    def test_query_progress_tracking
      @con.query('SET threads=1')
      @con.query('CREATE TABLE tbl as SELECT range a, mod(range, 10) b FROM range(10000)')
      @con.query('CREATE TABLE tbl2 as SELECT range a, FROM range(10000)')
      @con.query('SET ENABLE_PROGRESS_BAR=true')
      @con.query('SET ENABLE_PROGRESS_BAR_PRINT=false')

      pending_result = @con.async_query('SELECT count(*) FROM tbl where a = (SELECT min(a) FROM tbl2)')

      assert_equal(0, @con.query_progress.percentage)
      pending_result.execute_task while @con.query_progress.percentage.zero?

      assert_instance_of(DuckDB::QueryProgress, @con.query_progress)
    end

    def test_query_progress_values
      @con.query('SET threads=1')
      @con.query('CREATE TABLE tbl as SELECT range a, mod(range, 10) b FROM range(10000)')
      @con.query('CREATE TABLE tbl2 as SELECT range a, FROM range(10000)')
      @con.query('SET ENABLE_PROGRESS_BAR=true; SET ENABLE_PROGRESS_BAR_PRINT=false')

      pending_result = @con.async_query('SELECT count(*) FROM tbl where a = (SELECT min(a) FROM tbl2)')
      pending_result.execute_task while @con.query_progress.percentage.zero?
      query_progress = @con.query_progress

      assert_operator(query_progress.percentage, :>, 0)
      assert_operator(query_progress.rows_processed, :>, 0)
    end

    def test_query_progress_totals
      @con.query('SET threads=1')
      @con.query('CREATE TABLE tbl as SELECT range a, mod(range, 10) b FROM range(10000)')
      @con.query('CREATE TABLE tbl2 as SELECT range a, FROM range(10000)')
      @con.query('SET ENABLE_PROGRESS_BAR=true; SET ENABLE_PROGRESS_BAR_PRINT=false')

      pending_result = @con.async_query('SELECT count(*) FROM tbl where a = (SELECT min(a) FROM tbl2)')
      pending_result.execute_task while @con.query_progress.percentage.zero?
      query_progress = @con.query_progress

      assert_operator(query_progress.total_rows_to_process, :>, 0)
      assert_operator(query_progress.total_rows_to_process, :>=, query_progress.rows_processed)
    end

    def test_query_progress_interrupt
      @con.query('SET threads=1')
      @con.query('CREATE TABLE tbl as SELECT range a, mod(range, 10) b FROM range(10000)')
      @con.query('CREATE TABLE tbl2 as SELECT range a, FROM range(10000)')
      @con.query('SET ENABLE_PROGRESS_BAR=true; SET ENABLE_PROGRESS_BAR_PRINT=false')

      pending_result = @con.async_query('SELECT count(*) FROM tbl where a = (SELECT min(a) FROM tbl2)')
      @con.interrupt
      while pending_result.state == :not_ready
        pending_result.execute_task

        refute_equal(pending_result.state, :ready, 'pending_result.state should not be :ready')
      end
    end

    def test_disconnect
      @con.disconnect
      exception = assert_raises(DuckDB::Error) do
        @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      end
      assert_match(/Database connection closed/, exception.message)
    end

    def test_close
      @con.close
      exception = assert_raises(DuckDB::Error) do
        @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      end
      assert_match(/Database connection closed/, exception.message)
    end

    def test_connect
      @con.disconnect
      exception = assert_raises(DuckDB::Error) do
        @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      end
      assert_match(/Database connection closed/, exception.message)
      @con.connect(@db)

      assert_instance_of(DuckDB::Result, @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)'))
    end

    def test_connect_with_block
      @con.disconnect

      @con.connect(@db) do |con|
        assert_instance_of(DuckDB::Result, con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)'))
      end
      exception = assert_raises(DuckDB::Error) do
        @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      end
      assert_match(/Database connection closed/, exception.message)
    end

    def test_open
      @con.disconnect
      exception = assert_raises(DuckDB::Error) do
        @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      end
      assert_match(/Database connection closed/, exception.message)
      @con.open(@db)

      assert_instance_of(DuckDB::Result, @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)'))
    end

    def test_prepared_statement
      @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)')

      assert_instance_of(DuckDB::PreparedStatement, @con.prepared_statement('SELECT * FROM t WHERE col1 = $1'))
    end

    def test_prepare
      @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)')

      assert_instance_of(DuckDB::PreparedStatement, @con.prepare('SELECT * FROM t WHERE col1 = $1'))
    end

    def test_appender
      @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)')

      assert_instance_of(DuckDB::Appender, @con.appender('t'))
    end

    def test_appender_with_schema
      @con.execute('CREATE SCHEMA a; CREATE TABLE a.b (col1 INTEGER, col2 STRING)')

      assert_instance_of(DuckDB::Appender, @con.appender('a.b'))
    end

    def test_appender_with_block
      @con.execute('CREATE TABLE t (col1 INTEGER, col2 STRING)')
      @con.appender('t') do |appender|
        appender.append_row(1, 'foo')
      end
      r = @con.query('SELECT col1, col2 FROM t')

      assert_equal([1, 'foo'], r.first)
    end

    # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
    def test_appender_from_query
      skip 'not supported' unless DuckDB::Appender.respond_to?(:create_query)

      @con.query('CREATE TABLE t (i INT PRIMARY KEY, value VARCHAR)')
      @con.query("INSERT INTO t VALUES (1, 'hello')")

      query = 'INSERT OR REPLACE INTO t SELECT i, val FROM my_appended_data'
      types = [DuckDB::LogicalType::INTEGER, DuckDB::LogicalType::VARCHAR]
      appender = @con.appender_from_query(query, types, 'my_appended_data', %w[i val])

      appender.begin_row
      appender.append_int32(1)
      appender.append_varchar('hello world')
      appender.end_row
      appender.flush
      appender.begin_row
      appender.append_int32(2)
      appender.append_varchar('bye bye')
      appender.end_row
      appender.flush

      assert_equal([[1, 'hello world'], [2, 'bye bye']], @con.query('SELECT * FROM t ORDER BY i').to_a)
    end
    # rubocop:enable Metrics/AbcSize, Metrics/MethodLength

    # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
    def test_appender_from_query_omitting_args
      skip 'not supported' unless DuckDB::Appender.respond_to?(:create_query)

      @con.query('CREATE TABLE t (i INT PRIMARY KEY, value VARCHAR)')
      @con.query("INSERT INTO t VALUES (1, 'hello')")

      query = 'INSERT OR REPLACE INTO t SELECT col1, col2 FROM appended_data'
      types = [DuckDB::LogicalType::INTEGER, DuckDB::LogicalType::VARCHAR]
      appender = @con.appender_from_query(query, types)

      appender.begin_row
      appender.append_int32(1)
      appender.append_varchar('hello world')
      appender.end_row
      appender.flush
      appender.begin_row
      appender.append_int32(2)
      appender.append_varchar('bye bye')
      appender.end_row
      appender.flush

      assert_equal([[1, 'hello world'], [2, 'bye bye']], @con.query('SELECT * FROM t ORDER BY i').to_a)
    end
    # rubocop:enable Metrics/AbcSize, Metrics/MethodLength

    def test_appender_from_query_with_block
      skip 'not supported' unless DuckDB::Appender.respond_to?(:create_query)

      @con.query('CREATE TABLE t (i INT PRIMARY KEY, value VARCHAR)')
      @con.query("INSERT INTO t VALUES (1, 'hello')")

      query = 'INSERT OR REPLACE INTO t SELECT i, val FROM my_appended_data'
      types = [DuckDB::LogicalType::INTEGER, DuckDB::LogicalType::VARCHAR]

      @con.appender_from_query(query, types, 'my_appended_data', %w[i val]) do |appender|
        appender.append_row(1, 'hello world')
        appender.append_row(2, 'bye bye')
      end

      assert_equal([[1, 'hello world'], [2, 'bye bye']], @con.query('SELECT * FROM t ORDER BY i').to_a)
    end

    # Tests for register_scalar_function

    def test_register_scalar_function_inline_with_single_parameter
      @con.execute('SET threads=1')

      @con.register_scalar_function(
        name: :inline_triple,
        return_type: DuckDB::LogicalType::INTEGER,
        parameter_type: DuckDB::LogicalType::INTEGER
      ) { |v| v * 3 }

      result = @con.execute('SELECT inline_triple(7)')
      rows = result.to_a

      assert_equal 1, rows.size
      assert_equal 21, rows[0][0]
    end

    def test_register_scalar_function_inline_with_multiple_parameters
      @con.execute('SET threads=1')

      @con.register_scalar_function(
        name: :inline_add,
        return_type: DuckDB::LogicalType::INTEGER,
        parameter_types: [DuckDB::LogicalType::INTEGER, DuckDB::LogicalType::INTEGER]
      ) { |a, b| a + b }

      result = @con.execute('SELECT inline_add(15, 25)')
      rows = result.to_a

      assert_equal 1, rows.size
      assert_equal 40, rows[0][0]
    end

    def test_register_scalar_function_inline_with_no_parameters
      @con.execute('SET threads=1')

      @con.register_scalar_function(
        name: :inline_constant,
        return_type: DuckDB::LogicalType::INTEGER
      ) { 99 }

      result = @con.execute('SELECT inline_constant()')
      rows = result.to_a

      assert_equal 1, rows.size
      assert_equal 99, rows[0][0]
    end

    def test_register_scalar_function_inline_with_varchar
      @con.execute('SET threads=1')

      @con.register_scalar_function(
        name: :inline_reverse,
        return_type: DuckDB::LogicalType::VARCHAR,
        parameter_type: DuckDB::LogicalType::VARCHAR, &:reverse
      )

      result = @con.execute("SELECT inline_reverse('hello')")
      rows = result.to_a

      assert_equal 1, rows.size
      assert_equal 'olleh', rows[0][0]
    end

    def test_register_scalar_function_rejects_object_with_keywords # rubocop:disable Metrics/MethodLength
      @con.execute('SET threads=1')

      sf = DuckDB::ScalarFunction.create(
        name: :test,
        return_type: DuckDB::LogicalType::INTEGER
      ) { 42 }

      error = assert_raises(ArgumentError) do
        @con.register_scalar_function(
          sf,
          name: :duplicate
        )
      end

      assert_match(/cannot pass both/i, error.message)
    end

    def test_register_scalar_function_rejects_object_with_block
      @con.execute('SET threads=1')

      sf = DuckDB::ScalarFunction.create(
        name: :test,
        return_type: DuckDB::LogicalType::INTEGER
      ) { 42 }

      error = assert_raises(ArgumentError) do
        @con.register_scalar_function(sf) { 99 }
      end

      assert_match(/cannot pass both/i, error.message)
    end

    def test_register_scalar_function_object_style_still_works
      @con.execute('SET threads=1')

      sf = DuckDB::ScalarFunction.create(
        name: :object_style,
        return_type: DuckDB::LogicalType::INTEGER
      ) { 123 }

      @con.register_scalar_function(sf)

      result = @con.execute('SELECT object_style()')
      rows = result.to_a

      assert_equal 123, rows[0][0]
    end
  end
end
