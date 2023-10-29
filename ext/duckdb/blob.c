#include "ruby-duckdb.h"

VALUE cDuckDBBlob;

void rbduckdb_init_duckdb_blob(void) {
    cDuckDBBlob = rb_define_class_under(mDuckDB, "Blob", rb_cString);
}
