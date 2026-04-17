#include "ruby-duckdb.h"

VALUE cDuckDBDataChunk;
extern VALUE cDuckDBVector;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE initialize(int argc, VALUE *argv, VALUE self);
static VALUE rbduckdb_data_chunk_column_count(VALUE self);
static VALUE rbduckdb_data_chunk_get_size(VALUE self);
static VALUE rbduckdb_data_chunk_set_size(VALUE self, VALUE size);
static VALUE rbduckdb_data_chunk_get_vector(VALUE self, VALUE col_idx);
static VALUE rbduckdb_data_chunk__reset(VALUE self);

static const rb_data_type_t data_chunk_data_type = {
    "DuckDB/DataChunk",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBDataChunk *p = (rubyDuckDBDataChunk *)ctx;

    if (p->owned && p->data_chunk) {
        duckdb_destroy_data_chunk(&(p->data_chunk));
    }

    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBDataChunk *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBDataChunk));
    return TypedData_Wrap_Struct(klass, &data_chunk_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBDataChunk);
}

rubyDuckDBDataChunk *get_struct_data_chunk(VALUE obj) {
    rubyDuckDBDataChunk *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBDataChunk, &data_chunk_data_type, ctx);
    return ctx;
}

static VALUE initialize(int argc, VALUE *argv, VALUE self) {
    rubyDuckDBDataChunk *ctx;
    VALUE logical_types;
    idx_t column_count;
    duckdb_logical_type *types;
    long i;

    TypedData_Get_Struct(self, rubyDuckDBDataChunk, &data_chunk_data_type, ctx);

    rb_scan_args(argc, argv, "01", &logical_types);
    if (NIL_P(logical_types)) {
        return self;
    }

    Check_Type(logical_types, T_ARRAY);

    if (ctx->owned && ctx->data_chunk) {
        duckdb_destroy_data_chunk(&(ctx->data_chunk));
        ctx->owned = false;
    }

    column_count = (idx_t)RARRAY_LEN(logical_types);
    types = ALLOC_N(duckdb_logical_type, column_count);

    for (i = 0; i < RARRAY_LEN(logical_types); i++) {
        VALUE logical_type = rb_ary_entry(logical_types, i);
        rubyDuckDBLogicalType *logical_type_ctx = rbduckdb_get_struct_logical_type(logical_type);
        types[i] = logical_type_ctx->logical_type;
    }

    ctx->data_chunk = duckdb_create_data_chunk(types, column_count);
    xfree(types);

    if (!ctx->data_chunk) {
        rb_raise(eDuckDBError, "Failed to create data chunk");
    }

    ctx->owned = true;

    return self;
}

/*
 * call-seq:
 *   data_chunk.column_count -> Integer
 *
 * Returns the number of columns in the data chunk.
 *
 *   data_chunk.column_count  # => 2
 */
static VALUE rbduckdb_data_chunk_column_count(VALUE self) {
    rubyDuckDBDataChunk *ctx;
    idx_t count;

    TypedData_Get_Struct(self, rubyDuckDBDataChunk, &data_chunk_data_type, ctx);

    count = duckdb_data_chunk_get_column_count(ctx->data_chunk);

    return ULL2NUM(count);
}

/*
 * call-seq:
 *   data_chunk.size -> Integer
 *
 * Returns the current number of tuples in the data chunk.
 *
 *   data_chunk.size  # => 100
 */
static VALUE rbduckdb_data_chunk_get_size(VALUE self) {
    rubyDuckDBDataChunk *ctx;
    idx_t size;

    TypedData_Get_Struct(self, rubyDuckDBDataChunk, &data_chunk_data_type, ctx);

    size = duckdb_data_chunk_get_size(ctx->data_chunk);

    return ULL2NUM(size);
}

/*
 * call-seq:
 *   data_chunk.size = size -> size
 *
 * Sets the number of tuples in the data chunk.
 *
 *   data_chunk.size = 50
 */
static VALUE rbduckdb_data_chunk_set_size(VALUE self, VALUE size) {
    rubyDuckDBDataChunk *ctx;
    idx_t sz;

    TypedData_Get_Struct(self, rubyDuckDBDataChunk, &data_chunk_data_type, ctx);

    sz = NUM2ULL(size);
    duckdb_data_chunk_set_size(ctx->data_chunk, sz);

    return size;
}

/*
 * call-seq:
 *   data_chunk.get_vector(col_idx) -> DuckDB::Vector
 *
 * Gets the vector at the specified column index.
 *
 *   vector = data_chunk.get_vector(0)
 */
static VALUE rbduckdb_data_chunk_get_vector(VALUE self, VALUE col_idx) {
    rubyDuckDBDataChunk *ctx;
    idx_t idx;
    duckdb_vector vector;
    VALUE vector_obj;
    rubyDuckDBVector *vector_ctx;

    TypedData_Get_Struct(self, rubyDuckDBDataChunk, &data_chunk_data_type, ctx);

    idx = NUM2ULL(col_idx);
    vector = duckdb_data_chunk_get_vector(ctx->data_chunk, idx);

    // Create Vector wrapper
    vector_obj = rb_class_new_instance(0, NULL, cDuckDBVector);
    vector_ctx = get_struct_vector(vector_obj);
    vector_ctx->vector = vector;

    return vector_obj;
}

/*
 * call-seq:
 *   data_chunk._reset -> self
 *
 * Resets the data chunk, clearing its contents and setting its size to 0.
 *
 *   data_chunk._reset
 */
static VALUE rbduckdb_data_chunk__reset(VALUE self) {
    rubyDuckDBDataChunk *ctx;

    TypedData_Get_Struct(self, rubyDuckDBDataChunk, &data_chunk_data_type, ctx);

    duckdb_data_chunk_reset(ctx->data_chunk);

    return self;
}

void rbduckdb_init_duckdb_data_chunk(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBDataChunk = rb_define_class_under(mDuckDB, "DataChunk", rb_cObject);
    rb_define_alloc_func(cDuckDBDataChunk, allocate);

    rb_define_method(cDuckDBDataChunk, "initialize", initialize, -1);
    rb_define_method(cDuckDBDataChunk, "column_count", rbduckdb_data_chunk_column_count, 0);
    rb_define_method(cDuckDBDataChunk, "size", rbduckdb_data_chunk_get_size, 0);
    rb_define_method(cDuckDBDataChunk, "size=", rbduckdb_data_chunk_set_size, 1);
    rb_define_method(cDuckDBDataChunk, "get_vector", rbduckdb_data_chunk_get_vector, 1);
    rb_define_private_method(cDuckDBDataChunk, "_reset", rbduckdb_data_chunk__reset, 0);
}
