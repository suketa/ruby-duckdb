#include "ruby-duckdb.h"

VALUE cDuckDBVector;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE rbduckdb_vector_get_data(VALUE self);
static VALUE rbduckdb_vector_get_validity(VALUE self);
static VALUE rbduckdb_vector_assign_string_element(VALUE self, VALUE index, VALUE str);
static VALUE rbduckdb_vector_assign_string_element_len(VALUE self, VALUE index, VALUE str);
static VALUE rbduckdb_vector_logical_type(VALUE self);
static VALUE rbduckdb_vector_set_validity(VALUE self, VALUE index, VALUE valid);

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

/*
 * call-seq:
 *   vector.assign_string_element_len(index, str) -> self
 *
 * Assigns a string/blob value at the specified index with explicit length.
 * Supports strings containing null bytes (for BLOB columns).
 *
 *   vector.assign_string_element_len(0, "\x00\x01\x02\x03")
 */
static VALUE rbduckdb_vector_assign_string_element_len(VALUE self, VALUE index, VALUE str) {
    rubyDuckDBVector *ctx;
    idx_t idx;
    const char *string_val;
    idx_t str_len;

    TypedData_Get_Struct(self, rubyDuckDBVector, &vector_data_type, ctx);

    idx = NUM2ULL(index);
    string_val = StringValuePtr(str);
    str_len = RSTRING_LEN(str);

    duckdb_vector_assign_string_element_len(ctx->vector, idx, string_val, str_len);

    return self;
}

/*
 * call-seq:
 *   vector.logical_type -> DuckDB::LogicalType
 *
 * Gets the logical type of the vector.
 *
 *   vector = output.get_vector(0)
 *   type = vector.logical_type
 *   type.id  #=> DuckDB::Type::BIGINT
 */
static VALUE rbduckdb_vector_logical_type(VALUE self) {
    rubyDuckDBVector *ctx;
    duckdb_logical_type logical_type;

    TypedData_Get_Struct(self, rubyDuckDBVector, &vector_data_type, ctx);

    logical_type = duckdb_vector_get_column_type(ctx->vector);

    return rbduckdb_create_logical_type(logical_type);
}

/*
 * call-seq:
 *   vector.set_validity(index, valid) -> self
 *
 * Sets the validity of a value at the specified index.
 *
 *   vector.set_validity(0, false)  # Mark row 0 as NULL
 *   vector.set_validity(1, true)   # Mark row 1 as valid
 */
static VALUE rbduckdb_vector_set_validity(VALUE self, VALUE index, VALUE valid) {
    rubyDuckDBVector *ctx;
    idx_t idx;
    uint64_t *validity;

    TypedData_Get_Struct(self, rubyDuckDBVector, &vector_data_type, ctx);

    idx = NUM2ULL(index);

    if (RTEST(valid)) {
        // Setting to valid - ensure validity mask exists and set bit
        duckdb_vector_ensure_validity_writable(ctx->vector);
        validity = duckdb_vector_get_validity(ctx->vector);
        duckdb_validity_set_row_valid(validity, idx);
    } else {
        // Setting to invalid (NULL)
        duckdb_vector_ensure_validity_writable(ctx->vector);
        validity = duckdb_vector_get_validity(ctx->vector);
        duckdb_validity_set_row_invalid(validity, idx);
    }

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
    rb_define_method(cDuckDBVector, "assign_string_element_len", rbduckdb_vector_assign_string_element_len, 2);
    rb_define_method(cDuckDBVector, "logical_type", rbduckdb_vector_logical_type, 0);
    rb_define_method(cDuckDBVector, "set_validity", rbduckdb_vector_set_validity, 2);
}
