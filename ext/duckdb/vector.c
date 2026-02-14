#include "ruby-duckdb.h"

VALUE cDuckDBVector;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE rbduckdb_vector_get_data(VALUE self);
static VALUE rbduckdb_vector_get_validity(VALUE self);
static VALUE rbduckdb_vector_assign_string_element(VALUE self, VALUE index, VALUE str);

static const rb_data_type_t vector_data_type = {
    "DuckDB/Vector",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBVector *p = (rubyDuckDBVector *)ctx;
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBVector *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBVector));
    return TypedData_Wrap_Struct(klass, &vector_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBVector);
}

rubyDuckDBVector *get_struct_vector(VALUE obj) {
    rubyDuckDBVector *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBVector, &vector_data_type, ctx);
    return ctx;
}

/*
 * call-seq:
 *   vector.get_data -> Integer (pointer address)
 *
 * Gets the raw data pointer for the vector.
 * Returns the memory address as an integer.
 *
 *   ptr = vector.get_data
 */
static VALUE rbduckdb_vector_get_data(VALUE self) {
    rubyDuckDBVector *ctx;
    void *data;

    TypedData_Get_Struct(self, rubyDuckDBVector, &vector_data_type, ctx);

    data = duckdb_vector_get_data(ctx->vector);

    return ULL2NUM((uintptr_t)data);
}

/*
 * call-seq:
 *   vector.get_validity -> Integer or nil (pointer address)
 *
 * Gets the validity mask pointer for the vector.
 * Returns nil if all values are valid.
 *
 *   validity = vector.get_validity
 */
static VALUE rbduckdb_vector_get_validity(VALUE self) {
    rubyDuckDBVector *ctx;
    uint64_t *validity;

    TypedData_Get_Struct(self, rubyDuckDBVector, &vector_data_type, ctx);

    validity = duckdb_vector_get_validity(ctx->vector);

    if (!validity) {
        return Qnil;
    }

    return ULL2NUM((uintptr_t)validity);
}

/*
 * call-seq:
 *   vector.assign_string_element(index, str) -> self
 *
 * Assigns a string value at the specified index.
 *
 *   vector.assign_string_element(0, 'hello')
 */
static VALUE rbduckdb_vector_assign_string_element(VALUE self, VALUE index, VALUE str) {
    rubyDuckDBVector *ctx;
    idx_t idx;
    const char *string_val;

    TypedData_Get_Struct(self, rubyDuckDBVector, &vector_data_type, ctx);

    idx = NUM2ULL(index);
    string_val = StringValueCStr(str);

    duckdb_vector_assign_string_element(ctx->vector, idx, string_val);

    return self;
}

void rbduckdb_init_duckdb_vector(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBVector = rb_define_class_under(mDuckDB, "Vector", rb_cObject);
    rb_define_alloc_func(cDuckDBVector, allocate);

    rb_define_method(cDuckDBVector, "get_data", rbduckdb_vector_get_data, 0);
    rb_define_method(cDuckDBVector, "get_validity", rbduckdb_vector_get_validity, 0);
    rb_define_method(cDuckDBVector, "assign_string_element", rbduckdb_vector_assign_string_element, 2);
}
