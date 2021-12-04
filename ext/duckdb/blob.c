#include "ruby-duckdb.h"

#ifdef HAVE_DUCKDB_VALUE_BLOB

VALUE cDuckDBBlob;

void init_duckdb_blob(void) {
    cDuckDBBlob = rb_define_class_under(mDuckDB, "Blob", rb_cString);
}
#endif /* HAVE_DUCKDB_VALUE_BLOB */
