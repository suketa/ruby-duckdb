#include "ruby-duckdb.h"

VALUE mDuckDB;

static VALUE duckdb_s_library_version(VALUE self);

static VALUE duckdb_s_library_version(VALUE self) {
  return rb_str_new2(duckdb_library_version());
}

void
Init_duckdb_native(void) {
    mDuckDB = rb_define_module("DuckDB");

    rb_define_singleton_method(mDuckDB, "library_version", duckdb_s_library_version, 0);

    init_duckdb_error();
    init_duckdb_database();
    init_duckdb_connection();
    init_duckdb_result();
    init_duckdb_column();
    init_duckdb_prepared_statement();
    init_duckdb_blob();
    init_duckdb_appender();
    init_duckdb_config();
    init_duckdb_converter();
}
