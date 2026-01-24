# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class PendingResultTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      # @con.query('CREATE TABLE int_vals (int_val INTEGER)')
      # @con.query('INSERT INTO int_vals VALUES (1), (2), (3), (4), (5)')
      @con.query('SET threads=1')
      @con.query('CREATE TABLE tbl as SELECT range a, mod(range, 10) b FROM range(10000)')
      @con.query('CREATE TABLE tbl2 as SELECT range a, FROM range(10000)')
      @con.query('SET ENABLE_PROGRESS_BAR=true')
      @con.query('SET ENABLE_PROGRESS_BAR_PRINT=false')
      @stmt = @con.prepared_statement('SELECT count(*) FROM tbl where a = (SELECT min(a) FROM tbl2)')
      # pending_result = @con.async_query('SELECT count(*) FROM tbl where a = (SELECT min(a) FROM tbl2)')
    end

    def teardown
      @con.close
      @db.close
    end

    def test_state
      pending_result = @stmt.pending_prepared

      assert_equal :not_ready, pending_result.state

      pending_result.execute_task while pending_result.state == :not_ready

      assert_equal(:ready, pending_result.state)

      pending_result.execute_pending

      assert_equal(:ready, pending_result.state)

      pending_result.execute_task

      assert_equal(:error, pending_result.state)
    end

    def test_execution_finished?
      pending_result = @stmt.pending_prepared

      refute_predicate pending_result, :execution_finished?

      pending_result.execute_task while pending_result.state == :not_ready

      assert_predicate pending_result, :execution_finished?

      pending_result.execute_task

      assert_predicate pending_result, :execution_finished?
    end

    def test_execute_pending
      pending_result = @stmt.pending_prepared
      pending_result.execute_task while pending_result.state == :not_ready

      assert_equal :ready, pending_result.state
      assert_equal [[1]], pending_result.execute_pending.to_a
    end

    def test_execute_check_state
      pending_result = @stmt.pending_prepared
      state = pending_result.execute_check_state

      assert_equal(:no_tasks, state)

      pending_result.execute_task while pending_result.state == :not_ready

      state = pending_result.execute_check_state

      assert_includes(%i[error ready], state)
    end
  end
end
