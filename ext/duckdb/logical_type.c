#include "ruby-duckdb.h"

static VALUE cDuckDBLogicalType;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);

static const rb_data_type_t logical_type_data_type = {
    "DuckDB/LogicalType",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBLogicalType *p = (rubyDuckDBLogicalType *)ctx;

    if (p->logical_type) {
        duckdb_destroy_logical_type(&(p->logical_type));
    }

    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBLogicalType *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBLogicalType));
    return TypedData_Wrap_Struct(klass, &logical_type_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBLogicalType);
}

void rbduckdb_init_duckdb_logical_type(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBLogicalType = rb_define_class_under(mDuckDB, "LogicalType", rb_cObject);
    rb_define_alloc_func(cDuckDBLogicalType, allocate);
}
