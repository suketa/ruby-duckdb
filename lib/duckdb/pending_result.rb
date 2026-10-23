# frozen_string_literal: true

module DuckDB
  # The DuckDB::PendingResult encapsulates connection with DuckDB pending
  # result.
  # PendingResult provides methods to execute SQL asynchronousely and check
  # if the result is ready and to get the result.
  #
  #   require 'duckdb'
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
  #
  # Call +destroy+ to release a pending result without fetching it. On
  # DuckDB >= 2.0 an unfetched pending result blocks its connection until it
  # is destroyed. After +destroy+, +state+ (and +execution_finished?+) report
  # :error, and +execute_task+, +execute_pending+ and +execute_check_state+
  # raise DuckDB::Error. A DuckDB::Result already returned by
  # +execute_pending+ stays readable.
  class PendingResult
    STATES = %i[ready not_ready error no_tasks].freeze # :nodoc:

    # returns the state of the pending result.
    # the result can be :ready, :not_ready, :error, :no_tasks.
    #
    # :ready means the result is ready to be fetched, and
    # you can call `execute_pending` to get the result.
    #
    # :not_ready means the result is not ready yet, so
    # you need to call `execute_task`.
    #
    # @return [symbol] :ready, :not_ready, :error, :no_tasks
    def state
      STATES[_state]
    end

    # returns the state of the pending result.
    # the result can be :ready, :not_ready, :error, :no_tasks.
    #
    # :ready means the result is ready to be fetched, and
    # you can call `execute_pending` to get the result.
    #
    # :not_ready or :no_tasks might mean the pending result
    # is not executed yet, so you need to call `execute_task`.
    #
    # @return [symbol] :ready, :not_ready, :error, :no_tasks
    def execute_check_state
      STATES[_execute_check_state]
    end
  end
end
