#include "ruby-duckdb.h"

VALUE mDuckDB;
VALUE PositiveInfinity;
VALUE NegativeInfinity;

static VALUE duckdb_s_library_version(VALUE self);
static VALUE duckdb_s_vector_size(VALUE self);

/*
 * call-seq:
 *   DuckDB.library_version -> String
 *
 * Returns the version of the DuckDB library.
 *
 *   DuckDB.library_version # => "0.2.0"
 */
static VALUE duckdb_s_library_version(VALUE self) {
    return rb_str_new2(duckdb_library_version());
}

/*
 * call-seq:
 *   DuckDB.vector_size -> Integer
 *
 * Returns the vector size of DuckDB. The vector size is the number of rows
 * that are processed in a single vectorized operation.
 *
 *   DuckDB.vector_size # => 2048
 */
static VALUE duckdb_s_vector_size(VALUE self) {
    return ULONG2NUM(duckdb_vector_size());
}

void
Init_duckdb_native(void) {
    mDuckDB = rb_define_module("DuckDB");
    PositiveInfinity = rb_str_new_literal("infinity");
    NegativeInfinity = rb_str_new_literal("-infinity");

    rb_define_singleton_method(mDuckDB, "library_version", duckdb_s_library_version, 0);
    rb_define_singleton_method(mDuckDB, "vector_size", duckdb_s_vector_size, 0);

    rbduckdb_init_duckdb_error();
    rbduckdb_init_duckdb_database();
    rbduckdb_init_duckdb_connection();
    rbduckdb_init_duckdb_result();
    rbduckdb_init_duckdb_column();
    rbduckdb_init_duckdb_logical_type();
    rbduckdb_init_duckdb_prepared_statement();
    rbduckdb_init_duckdb_pending_result();
    rbduckdb_init_duckdb_blob();
    rbduckdb_init_duckdb_appender();
    rbduckdb_init_duckdb_config();
    rbduckdb_init_duckdb_converter();
    rbduckdb_init_duckdb_extracted_statements();
    rbduckdb_init_duckdb_instance_cache();
    rbduckdb_init_duckdb_value_impl();
    rbduckdb_init_duckdb_scalar_function();
    rbduckdb_init_duckdb_function_info();
    rbduckdb_init_duckdb_vector();
    rbduckdb_init_duckdb_data_chunk();
    rbduckdb_init_memory_helper();
    rbduckdb_init_duckdb_table_function();
    rbduckdb_init_duckdb_bind_info();
    rbduckdb_init_duckdb_table_function_init_info();
}
