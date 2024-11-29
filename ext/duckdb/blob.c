#include "ruby-duckdb.h"

VALUE cDuckDBBlob;

void rbduckdb_init_duckdb_blob(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBBlob = rb_define_class_under(mDuckDB, "Blob", rb_cString);
}
