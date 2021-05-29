#include "ruby-duckdb.h"

VALUE eDuckDBError;

void init_duckdb_error(void)
{
    eDuckDBError = rb_define_class_under(mDuckDB, "Error", rb_eStandardError);
}
