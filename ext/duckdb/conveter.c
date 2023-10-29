#include "ruby-duckdb.h"

VALUE mDuckDBConverter;

void rbduckdb_init_duckdb_converter(void) {
    mDuckDBConverter = rb_define_module_under(mDuckDB, "Converter");
}
