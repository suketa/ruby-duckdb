#include "ruby-duckdb.h"

VALUE cDuckDBBlob;

void init_duckdb_blob(void) {
    cDuckDBBlob = rb_define_class_under(mDuckDB, "Blob", rb_cString);
}
