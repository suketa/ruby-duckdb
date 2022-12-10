#include "ruby-duckdb.h"

VALUE mDuckDB;

#ifdef HAVE_DUCKDB_H_GE_V060
static VALUE duckdb_s_library_version(VALUE self);
#endif

#ifdef HAVE_DUCKDB_H_GE_V060
static VALUE duckdb_s_library_version(VALUE self) {
  return rb_str_new2(duckdb_library_version());
}
#endif

void
Init_duckdb_native(void) {
    mDuckDB = rb_define_module("DuckDB");

#ifdef HAVE_DUCKDB_H_GE_V060
    rb_define_singleton_method(mDuckDB, "library_version", duckdb_s_library_version, 0);
#endif

    init_duckdb_error();
    init_duckdb_database();
    init_duckdb_connection();
    init_duckdb_result();
    init_duckdb_column();
    init_duckdb_prepared_statement();
    init_duckdb_blob();
    init_duckdb_appender();
    init_duckdb_config();

}
