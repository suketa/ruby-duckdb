#include "ruby-duckdb.h"

VALUE cDuckDBDataChunk;
extern VALUE cDuckDBVector;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE rbduckdb_data_chunk_column_count(VALUE self);
static VALUE rbduckdb_data_chunk_get_size(VALUE self);
static VALUE rbduckdb_data_chunk_set_size(VALUE self, VALUE size);
static VALUE rbduckdb_data_chunk_get_vector(VALUE self, VALUE col_idx);

static const rb_data_type_t data_chunk_data_type = {
    "DuckDB/DataChunk",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBDataChunk *p = (rubyDuckDBDataChunk *)ctx;
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

void rbduckdb_init_duckdb_data_chunk(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBDataChunk = rb_define_class_under(mDuckDB, "DataChunk", rb_cObject);
    rb_define_alloc_func(cDuckDBDataChunk, allocate);

    rb_define_method(cDuckDBDataChunk, "column_count", rbduckdb_data_chunk_column_count, 0);
    rb_define_method(cDuckDBDataChunk, "size", rbduckdb_data_chunk_get_size, 0);
    rb_define_method(cDuckDBDataChunk, "size=", rbduckdb_data_chunk_set_size, 1);
    rb_define_method(cDuckDBDataChunk, "get_vector", rbduckdb_data_chunk_get_vector, 1);
}
