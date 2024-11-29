#include "ruby-duckdb.h"

VALUE eDuckDBError;

void rbduckdb_init_duckdb_error(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    eDuckDBError = rb_define_class_under(mDuckDB, "Error", rb_eStandardError);
}
