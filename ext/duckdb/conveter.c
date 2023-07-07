#include "ruby-duckdb.h"

VALUE mDuckDBConverter;

void init_duckdb_converter(void) {
    mDuckDBConverter = rb_define_module_under(mDuckDB, "Converter");
}
