# frozen_string_literal: true

module DuckDB
  # The DuckDB::PendingResult encapsulates connection with DuckDB pending
  # result.
  # PendingResult provides methods to execute SQL asynchronousely and check
  # if the result is ready and to get the result.
  #
  #   require 'duckdb'
  #
  #   DuckDB::Result.use_chunk_each = true
  #
  #   db = DuckDB::Database.open
  #   con = db.connect
  #   stmt = con.prepared_statement(VERY_SLOW_QUERY)
  #   pending_result = stmt.pending_prepared
  #   while pending_result.state == :not_ready
  #     print '.'
  #     sleep(0.01)
  #     pending_result.execute_task
  #   end
  #   result = pending_result.execute_pending
  class PendingResult
    STATES = %i[ready not_ready error no_tasks].freeze

    # returns the state of the pending result.
    # The result can be :ready, :not_ready, :error, :no_tasks.
    # (:no_tasks is available only with duckdb 0.9.0 or later.)
    #
    # :ready means the result is ready to be fetched, and
    # you can call `execute_pending` to get the result.
    #
    # :not_ready means the result is not ready yet, so
    # you need to call `execute_task`.
    #
    # @return [Symbol] :ready, :not_ready, :error, :no_tasks
    def state
      STATES[_state]
    end
  end
end
