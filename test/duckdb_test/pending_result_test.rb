module DuckDBTest
  class PendingResultTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      @con.query('CREATE TABLE int_vals (int_val INTEGER)')
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

    def teardown
      @con.close
      @db.close
    end
  end
end
