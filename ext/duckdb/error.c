#include "ruby-duckdb.h"

VALUE eDuckDBError;

/* Raises DuckDB::Error from a failed result. C only reads the message and error
 * type id off the result (the result is destroyed before Ruby sees it) and passes
 * them to DuckDB::Error.new(message, error_type_id). */
void rbduckdb_raise_result_error(duckdb_result *result) {
    const char *msg = duckdb_result_error(result);
    if (!msg) {
        msg = "DuckDB error";
    }
    VALUE exc = rb_funcall(eDuckDBError, rb_intern("new"), 2, rb_str_new_cstr(msg), INT2FIX(duckdb_result_error_type(result)));
    rb_exc_raise(exc);
}

void rbduckdb_init_error(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    eDuckDBError = rb_define_class_under(mDuckDB, "Error", rb_eStandardError);
}
