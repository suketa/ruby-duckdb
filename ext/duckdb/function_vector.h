#ifndef RUBY_DUCKDB_FUNCTION_VECTOR_H
#define RUBY_DUCKDB_FUNCTION_VECTOR_H

/*
 * Shared vector-write helper for UDF callbacks.
 *
 * Converts a Ruby VALUE to the appropriate DuckDB type and writes it
 * into a result vector at the given index.  Used by both ScalarFunction
 * and AggregateFunction finalize paths.
 */

/*
 * Write `value` (a Ruby VALUE) into `vector` at position `index`.
 *
 * `element_type` is the logical type of the vector and determines
 * which conversion is applied.  If `value` is Qnil the row is marked
 * invalid (NULL).
 *
 * Raises rb_eArgError for unsupported types - callers that cannot
 * tolerate longjmp should wrap the call in rb_protect.
 */
void rbduckdb_vector_set_value_at(duckdb_vector vector,
                                  duckdb_logical_type element_type,
                                  idx_t index,
                                  VALUE value);

#endif
