# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class PendingResultTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      @con.query('CREATE TABLE int_vals (int_val INTEGER)')
      @con.query('INSERT INTO int_vals VALUES (1), (2), (3), (4), (5)')
      @stmt = @con.prepared_statement('SELECT * FROM int_vals')
    end

    def test_state
      pending_result = @stmt.pending_prepared
      assert_equal :not_ready, pending_result.state
      pending_result.execute_task
      sleep 0.01
      pending_result.execute_task
      assert_equal :ready, pending_result.state
    end

    def test_execution_finished?
      pending_result = @stmt.pending_prepared
      skip 'execution_finished? is not available' unless pending_result.respond_to? :execution_finished?

      assert_equal false, pending_result.execution_finished?
      pending_result.execute_task
      expected = pending_result.state == :ready
      assert_equal expected, pending_result.execution_finished?

      sleep 0.01

      pending_result.execute_task
      expected = pending_result.state == :ready
      assert_equal expected, pending_result.execution_finished?
    end

    def test_execute_pending
      pending_result = @stmt.pending_prepared
      while pending_result.state != :ready
        sleep 0.01
        pending_result.execute_task
      end
      assert_equal :ready, pending_result.state
      assert_equal [[1], [2], [3], [4], [5]], pending_result.execute_pending.to_a
    end

    def test_execute_check_state
      pending_result = @stmt.pending_prepared
      assert_equal(:no_tasks, pending_result.execute_check_state)
      pending_result.execute_task
      assert_equal(:no_tasks, pending_result.execute_check_state)
      sleep 0.01
      assert_equal(:ready, pending_result.execute_check_state)
      pending_result.execute_task
      assert_equal(:ready, pending_result.execute_check_state)
    end

    def teardown
      @con.close
      @db.close
    end
  end
end
