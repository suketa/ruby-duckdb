#include "ruby-duckdb.h"

static VALUE cDuckDBExtractedStatements;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);

static const rb_data_type_t extract_statements_data_type = {
    "DuckDB/ExtractedStatements",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBExtractedStatements *p = (rubyDuckDBExtractedStatements *)ctx;

    duckdb_destroy_extracted(&(p->extracted_statements));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBExtractedStatements *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBExtractedStatements));
    return TypedData_Wrap_Struct(klass, &extract_statements_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBExtractedStatements);
}

void rbduckdb_init_duckdb_extract_statements(void) {
    cDuckDBExtractedStatements = rb_define_class_under(mDuckDB, "ExtractedStatements", rb_cObject);
}
