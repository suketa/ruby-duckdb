#include "ruby-duckdb.h"

VALUE cDuckDBValueImpl;

static void deallocate(void *);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);

static const rb_data_type_t value_impl_data_type = {
    "DuckDB/ValueImpl",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void * ctx) {
    rubyDuckDBValueImpl *p = (rubyDuckDBValueImpl *)ctx;

    duckdb_destroy_value(&(p->value));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBValueImpl *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBValueImpl));
    return TypedData_Wrap_Struct(klass, &value_impl_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBValueImpl);
}

VALUE rbduckdb_value_impl_new(duckdb_value value) {
    rubyDuckDBValueImpl *ctx;
    VALUE obj = allocate(cDuckDBValueImpl);
    TypedData_Get_Struct(obj, rubyDuckDBValueImpl, &value_impl_data_type, ctx);
    ctx->value = value;
    return obj;
}

void rbduckdb_init_duckdb_value_impl(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBValueImpl = rb_define_class_under(mDuckDB, "ValueImpl", rb_cObject);
    rb_define_alloc_func(cDuckDBValueImpl, allocate);
}

