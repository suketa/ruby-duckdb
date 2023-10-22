module DuckDB
  class PendingResult
    STATE = %i[ready not_ready error no_tasks].freeze

    def state
      STATE[_state]
    end
  end
end
