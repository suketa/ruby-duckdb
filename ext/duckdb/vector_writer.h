#ifndef RUBY_DUCKDB_VECTOR_WRITER_H
#define RUBY_DUCKDB_VECTOR_WRITER_H

/*
 * Ruby value -> DuckDB vector cell writer.
 *
 * Converts a Ruby VALUE to the appropriate DuckDB type and writes it
 * into a vector at the given index.  This is the single place that
 * decides how a Ruby object is written into a vector: ScalarFunction
 * and AggregateFunction output, DataChunk#set_value (TableFunction
 * output, Appender#append_data_chunk) all go through it.
 *
 * TIMESTAMP (and TIMESTAMP_S/MS/NS) stores the wall-clock components of
 * the Time as-is, like PreparedStatement#bind_timestamp and
 * Appender#append_timestamp.  TIMESTAMP_TZ stores the instant
 * (microseconds since the Unix epoch), so the Time's utc_offset is
 * taken into account.
 */

/*
 * Write `value` (a Ruby VALUE) into `vector` at position `index`.
 *
 * `element_type` is the logical type of the vector and determines
 * which conversion is applied.  If `value` is Qnil the row is marked
 * invalid (NULL).
 *
 * Raises ArgumentError for unsupported types - callers that cannot
 * tolerate longjmp should wrap the call in rb_protect.
 */
void rbduckdb_vector_set_value_at(duckdb_vector vector,
                                  duckdb_logical_type element_type,
                                  idx_t index,
                                  VALUE value);

/*
 * Load the vector and logical type of each of the `n` columns of `input`.
 * Allocates both arrays with ALLOC_N; release them with
 * rbduckdb_input_columns_free.
 */
void rbduckdb_input_columns_load(duckdb_data_chunk input,
                                 idx_t n,
                                 duckdb_vector **vectors,
                                 duckdb_logical_type **types);

/*
 * Destroy the `n` logical types and free both arrays.  NULL arrays are
 * tolerated so it is safe to call when the load never happened.
 */
void rbduckdb_input_columns_free(duckdb_vector *vectors,
                                 duckdb_logical_type *types,
                                 idx_t n);

#endif
