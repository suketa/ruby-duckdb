# frozen_string_literal: true

module DuckDB
  # DuckDB::AggregateFunction lets you register a custom aggregate function
  # written in Ruby and call it from SQL.
  #
  # An aggregate function folds many rows into a single value. You define its
  # behaviour with four callbacks:
  #
  # * +set_init+    — called once per group; returns the initial state.
  # * +set_update+  — called once per row; receives the current state and the
  #                   input value(s), returns the new state.
  # * +set_combine+ — merges two partial states (required for parallel
  #                   execution); receives source and target states, returns the
  #                   merged state.
  # * +set_finalize+ — converts the final state into the SQL result value.
  #
  # Only +set_init+ is required. The other three have sensible defaults:
  # * +set_update+   defaults to +{ |state, *| state }+ (ignore inputs)
  # * +set_combine+  defaults to +{ |s1, _s2| s1 }+ (keep source state)
  # * +set_finalize+ defaults to +{ |x| x }+ (return state as-is)
  #
  # @note The default +set_combine+ keeps the source state and discards the
  #   target, which is only correct for single-threaded (single-partition)
  #   execution. If DuckDB runs the aggregate in parallel it will produce
  #   wrong results. Always supply an explicit +set_combine+ when the
  #   aggregate must be parallel-safe.
  #
  # == Basic example: custom SUM
  #
  #   af = DuckDB::AggregateFunction.new
  #   af.name        = 'my_sum'
  #   af.return_type = DuckDB::LogicalType::BIGINT
  #   af.add_parameter(DuckDB::LogicalType::BIGINT)
  #
  #   af.set_init    { 0 }
  #   af.set_update  { |state, value| state + value }
  #   af.set_combine { |s1, s2| s1 + s2 }
  #
  #   con.register_aggregate_function(af)
  #   con.query('SELECT my_sum(i) FROM range(100) t(i)').first.first  # => 4950
  #
  # == Example: weighted average with Hash state
  #
  #   af = DuckDB::AggregateFunction.new
  #   af.name        = 'weighted_avg'
  #   af.return_type = DuckDB::LogicalType::DOUBLE
  #   af.add_parameter(DuckDB::LogicalType::DOUBLE)  # value
  #   af.add_parameter(DuckDB::LogicalType::DOUBLE)  # weight
  #
  #   af.set_init    { { sum: 0.0, weight: 0.0 } }
  #   af.set_update  { |state, value, weight| { sum: state[:sum] + value * weight, weight: state[:weight] + weight } }
  #   af.set_combine { |s1, s2| { sum: s1[:sum] + s2[:sum], weight: s1[:weight] + s2[:weight] } }
  #   af.set_finalize { |state| state[:weight].zero? ? nil : state[:sum] / state[:weight] }
  #
  #   con.register_aggregate_function(af)
  class AggregateFunction
    include FunctionTypeValidation

    class << self
      def create( # rubocop:disable Metrics/MethodLength, Metrics/ParameterLists
        name:,
        return_type:,
        params: [], # rubocop:disable Style/KeywordParametersOrder
        init:,
        update: ->(state, *_inputs) { state },
        combine: ->(state, _other_state) { state },
        finalize: ->(state) { state },
        null_handling: false
      )
        callable!(:init, init)
        callable!(:update, update)
        callable!(:combine, combine)
        callable!(:finalize, finalize)

        af = AggregateFunction.new
        af.name = name
        af.return_type = return_type
        params.each do |param|
          af.add_parameter(param)
        end
        af.set_init(&init)
        af.set_update(&update)
        af.set_combine(&combine)
        af.set_finalize(&finalize)
        af.set_special_handling if null_handling
        af
      end

      private

      def callable!(name, arg)
        raise ArgumentError, "#{name} must respond to `call`" unless arg.respond_to?(:call)
      end
    end

    # Sets the return type for the aggregate function.
    #
    # @param logical_type [DuckDB::LogicalType | :logical_type_symbol] the return type
    # @return [DuckDB::AggregateFunction] self
    # @raise [DuckDB::Error] if the type is not supported
    def return_type=(logical_type)
      logical_type = check_supported_type!(logical_type)

      _set_return_type(logical_type)
    end

    # Adds a parameter to the aggregate function.
    #
    # @param logical_type [DuckDB::LogicalType | :logical_type_symbol] the parameter type
    # @return [DuckDB::AggregateFunction] self
    # @raise [DuckDB::Error] if the type is not supported
    def add_parameter(logical_type)
      logical_type = check_supported_type!(logical_type)

      _add_parameter(logical_type)
    end

    # Sets the block that initialises the per-group state.
    # The block takes no arguments and returns the initial state value.
    # This is the only required callback; defaults for +set_update+,
    # +set_combine+, and +set_finalize+ are injected automatically on the
    # first call if those methods have not been called explicitly.
    #
    # @note The injected default for +set_combine+ is +{ |s1, _s2| s1 }+, which
    #   is only correct for single-threaded execution. Always call +set_combine+
    #   explicitly when the aggregate must be parallel-safe.
    #
    # @return [DuckDB::AggregateFunction] self
    def set_init(&)
      unless @init_set
        _set_update { |state, *| state } unless @update_set
        _set_combine { |s1, _s2| s1 } unless @combine_set
        _set_finalize { |x| x } unless @finalize_set
      end
      _set_init(&)
      @init_set = true
    end

    # Sets the block that accumulates one row into the state.
    # The block receives the current state followed by the input column
    # value(s) for that row, and must return the updated state.
    # Default: +{ |state, *| state }+ (ignore inputs, keep state unchanged).
    # May be called after +set_init+ to override the injected default.
    #
    # @return [DuckDB::AggregateFunction] self
    def set_update(&)
      @update_set = true
      _set_update(&)
    end

    # Sets the block that merges two partial states during parallel execution.
    # The block receives the source and target states and must return the
    # merged state.
    # May be called after +set_init+ to override the injected default.
    #
    # @note The default +{ |s1, _s2| s1 }+ is only correct for single-threaded
    #   execution. Supply an explicit combine block for parallel-safe aggregates.
    #
    # @return [DuckDB::AggregateFunction] self
    def set_combine(&)
      @combine_set = true
      _set_combine(&)
    end

    # Sets the block that converts the final state into the SQL result value.
    # The block receives the accumulated state and must return a value
    # compatible with the declared +return_type+.
    # Default: +{ |x| x }+ (return the state as-is).
    # May be called after +set_init+ to override the injected default.
    #
    # @return [DuckDB::AggregateFunction] self
    def set_finalize(&)
      @finalize_set = true
      _set_finalize(&)
    end

    # Sets special NULL handling for the aggregate function.
    # By default DuckDB skips rows with NULL input values.  Calling this
    # method disables that behaviour so the update callback is invoked even
    # when inputs are NULL, receiving +nil+ for each NULL argument.  This
    # lets the function implement its own NULL semantics (e.g. counting
    # NULLs).
    #
    # Wraps +duckdb_aggregate_function_set_special_handling+.
    #
    # @return [DuckDB::AggregateFunction] self
    def set_special_handling
      _set_special_handling
    end
  end
end
