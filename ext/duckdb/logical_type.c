#include "ruby-duckdb.h"

static VALUE cDuckDBLogicalType;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE duckdb_logical_type__type(VALUE self);
static VALUE duckdb_logical_type_width(VALUE self);
static VALUE duckdb_logical_type_scale(VALUE self);

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

/*
 *  call-seq:
 *    decimal_col.logical_type.type -> Symbol
 *
 *  Returns the logical type's type symbol.
 *
 */
static VALUE duckdb_logical_type__type(VALUE self) {
    rubyDuckDBLogicalType *ctx;
    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);
    return INT2FIX(duckdb_get_type_id(ctx->logical_type));
}

/*
 *  call-seq:
 *    decimal_col.logical_type.width -> Integer
 *
 *  Returns the width of the decimal column.
 *
 */
static VALUE duckdb_logical_type_width(VALUE self) {
    rubyDuckDBLogicalType *ctx;
    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);
    return INT2FIX(duckdb_decimal_width(ctx->logical_type));
}

/*
 *  call-seq:
 *    decimal_col.logical_type.scale -> Integer
 *
 *  Returns the scale of the decimal column.
 *
 */
static VALUE duckdb_logical_type_scale(VALUE self) {
    rubyDuckDBLogicalType *ctx;
    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);
    return INT2FIX(duckdb_decimal_scale(ctx->logical_type));
}

VALUE rbduckdb_create_logical_type(duckdb_logical_type logical_type) {
    VALUE obj;
    rubyDuckDBLogicalType *ctx;

    obj = allocate(cDuckDBLogicalType);
    TypedData_Get_Struct(obj, rubyDuckDBLogicalType, &logical_type_data_type, ctx);

    ctx->logical_type = logical_type;

    return obj;
}

void rbduckdb_init_duckdb_logical_type(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBLogicalType = rb_define_class_under(mDuckDB, "LogicalType", rb_cObject);
    rb_define_alloc_func(cDuckDBLogicalType, allocate);

    rb_define_private_method(cDuckDBLogicalType, "_type", duckdb_logical_type__type, 0);
    rb_define_method(cDuckDBLogicalType, "width", duckdb_logical_type_width, 0);
    rb_define_method(cDuckDBLogicalType, "scale", duckdb_logical_type_scale, 0);
}
