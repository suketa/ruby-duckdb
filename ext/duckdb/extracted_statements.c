#include "ruby-duckdb.h"

static VALUE cDuckDBExtractedStatements;

void rbduckdb_init_duckdb_extract_statements(void) {
    cDuckDBExtractedStatements = rb_define_class_under(mDuckDB, "ExtractedStatements", rb_cObject);
}
