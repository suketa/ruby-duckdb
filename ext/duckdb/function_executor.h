#ifndef RUBY_DUCKDB_FUNCTION_EXECUTOR_H
#define RUBY_DUCKDB_FUNCTION_EXECUTOR_H

/*
 * Shared executor-thread dispatcher for UDF callbacks.
 *
 * DuckDB invokes UDF callbacks from its own worker threads, which are NOT
 * Ruby threads. Ruby's GVL cannot be acquired from a non-Ruby thread via
 * rb_thread_call_with_gvl (it crashes with rb_bug). This module provides
 * a dispatcher that routes callbacks to a single global "executor" Ruby
 * thread, so that the GVL can be obtained safely.
 *
 * Both ScalarFunction and AggregateFunction (future) share this
 * infrastructure.
 */

/*
 * A generic callback to be executed with the GVL held.
 * The user_data pointer is passed through unchanged from the dispatch call.
 *
 * The callback is responsible for catching Ruby exceptions (e.g. via
 * rb_protect) if needed — this module does not perform exception handling.
 */
typedef void (*rbduckdb_function_callback_t)(void *user_data);

/*
 * Start the global executor thread (idempotent).
 *
 * Must be called from a Ruby thread (GVL held). The GVL serializes calls,
 * so the internal check-then-set is safe without an additional mutex.
 */
void rbduckdb_function_executor_ensure_started(void);

/*
 * Dispatch a callback for execution with the GVL held. Automatically
 * selects one of three paths:
 *
 *   1. Called from a Ruby thread with GVL    -> invoke directly
 *   2. Called from a Ruby thread without GVL -> rb_thread_call_with_gvl
 *   3. Called from a non-Ruby thread         -> enqueue to executor thread
 *
 * Blocks until the callback returns.
 */
void rbduckdb_function_executor_dispatch(rbduckdb_function_callback_t cb, void *user_data);

#endif
