#include "ruby-duckdb.h"

VALUE mDuckDB;

void
Init_duckdb_native(void)
{
    mDuckDB = rb_define_module("DuckDB");

    init_duckdb_error();
    init_duckdb_database();
    init_duckdb_connection();
    init_duckdb_result();
    init_duckdb_prepared_statement();

#ifdef HAVE_DUCKDB_VALUE_BLOB

    init_duckdb_blob();

#endif /* HAVE_DUCKDB_VALUE_BLOB */

#ifdef HAVE_DUCKDB_APPENDER_CREATE

    init_duckdb_appender();

#endif /* HAVE_DUCKDB_APPENDER_CREATE */
}
