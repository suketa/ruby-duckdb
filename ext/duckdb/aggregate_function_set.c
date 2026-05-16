#include "ruby-duckdb.h"

VALUE cDuckDBAggregateFunctionSet;

static void mark(void *);
static void deallocate(void *);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static void compact(void *);
static VALUE rbduckdb_aggregate_function_set__initialize(VALUE self, VALUE name);
static VALUE rbduckdb_aggregate_function_set__add(VALUE self, VALUE aggregate_function);

static const rb_data_type_t aggregate_function_set_data_type = {
    "DuckDB/AggregateFunctionSet",
    {mark, deallocate, memsize, compact},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void mark(void *ctx) {
    rubyDuckDBAggregateFunctionSet *p = (rubyDuckDBAggregateFunctionSet *)ctx;
    rb_gc_mark(p->functions);
}

static void deallocate(void *ctx) {
    rubyDuckDBAggregateFunctionSet *p = (rubyDuckDBAggregateFunctionSet *)ctx;
    duckdb_destroy_aggregate_function_set(&(p->aggregate_function_set));
    xfree(p);
}

static void compact(void *ctx) {
    rubyDuckDBAggregateFunctionSet *p = (rubyDuckDBAggregateFunctionSet *)ctx;
    p->functions = rb_gc_location(p->functions);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBAggregateFunctionSet *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBAggregateFunctionSet));
    VALUE obj = TypedData_Wrap_Struct(klass, &aggregate_function_set_data_type, ctx);
    ctx->functions = rb_ary_new();
    RB_GC_GUARD(ctx->functions);
    return obj;
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBAggregateFunctionSet);
}

rubyDuckDBAggregateFunctionSet *get_struct_aggregate_function_set(VALUE obj) {
    rubyDuckDBAggregateFunctionSet *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBAggregateFunctionSet, &aggregate_function_set_data_type, ctx);
    return ctx;
}

/* :nodoc: */
static VALUE rbduckdb_aggregate_function_set__initialize(VALUE self, VALUE name) {
    rubyDuckDBAggregateFunctionSet *p;

    TypedData_Get_Struct(self, rubyDuckDBAggregateFunctionSet, &aggregate_function_set_data_type, p);
    p->aggregate_function_set = duckdb_create_aggregate_function_set(StringValueCStr(name));
    return self;
}

/* :nodoc: */
static VALUE rbduckdb_aggregate_function_set__add(VALUE self, VALUE aggregate_function) {
    rubyDuckDBAggregateFunctionSet *p;
    rubyDuckDBAggregateFunction *af;

    TypedData_Get_Struct(self, rubyDuckDBAggregateFunctionSet, &aggregate_function_set_data_type, p);
    af = rbduckdb_get_struct_aggregate_function(aggregate_function);

    if (duckdb_add_aggregate_function_to_set(p->aggregate_function_set, af->aggregate_function) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to add aggregate function to set (duplicate overload?)");
    }

    rb_ary_push(p->functions, aggregate_function);
    return self;
}

void rbduckdb_init_duckdb_aggregate_function_set(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBAggregateFunctionSet = rb_define_class_under(mDuckDB, "AggregateFunctionSet", rb_cObject);
    rb_define_alloc_func(cDuckDBAggregateFunctionSet, allocate);
    rb_define_private_method(cDuckDBAggregateFunctionSet, "_initialize", rbduckdb_aggregate_function_set__initialize, 1);
    rb_define_private_method(cDuckDBAggregateFunctionSet, "_add", rbduckdb_aggregate_function_set__add, 1);
}
