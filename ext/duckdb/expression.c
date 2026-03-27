#include "ruby-duckdb.h"

VALUE cDuckDBExpression;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);

static const rb_data_type_t expression_data_type = {
    "DuckDB/Expression",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBExpression *p = (rubyDuckDBExpression *)ctx;

    if (p->expression) {
        duckdb_destroy_expression(&(p->expression));
    }

    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBExpression *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBExpression));
    return TypedData_Wrap_Struct(klass, &expression_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBExpression);
}

VALUE rbduckdb_expression_new(duckdb_expression expression) {
    rubyDuckDBExpression *ctx;
    VALUE obj = allocate(cDuckDBExpression);

    TypedData_Get_Struct(obj, rubyDuckDBExpression, &expression_data_type, ctx);
    ctx->expression = expression;

    return obj;
}

void rbduckdb_init_duckdb_expression(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBExpression = rb_define_class_under(mDuckDB, "Expression", rb_cObject);
    rb_define_alloc_func(cDuckDBExpression, allocate);
}
