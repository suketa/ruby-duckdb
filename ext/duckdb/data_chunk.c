#include "ruby-duckdb.h"

VALUE cDuckDBDataChunk;
extern VALUE cDuckDBVector;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE data_chunk_initialize(int argc, VALUE *argv, VALUE self);
static VALUE data_chunk_column_count(VALUE self);
static VALUE data_chunk_size(VALUE self);
static VALUE data_chunk_set_size(VALUE self, VALUE size);
static VALUE data_chunk_get_vector(VALUE self, VALUE col_idx);
static VALUE data_chunk__reset(VALUE self);
static VALUE data_chunk_set_value(VALUE self, VALUE col_idx, VALUE row_idx, VALUE value);
static void free_column_types(rubyDuckDBDataChunk *ctx);
static duckdb_logical_type column_type(rubyDuckDBDataChunk *ctx, idx_t col_idx);

static const rb_data_type_t data_chunk_data_type = {
    "DuckDB/DataChunk",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY | RUBY_TYPED_WB_PROTECTED
};

static void free_column_types(rubyDuckDBDataChunk *ctx) {
    idx_t i;

    if (ctx->column_types == NULL) {
        return;
    }
    for (i = 0; i < ctx->column_types_count; i++) {
        if (ctx->column_types[i] != NULL) {
            duckdb_destroy_logical_type(&(ctx->column_types[i]));
        }
    }
    xfree(ctx->column_types);
    ctx->column_types = NULL;
    ctx->column_types_count = 0;
}

static void deallocate(void *ctx) {
    rubyDuckDBDataChunk *p = (rubyDuckDBDataChunk *)ctx;

    free_column_types(p);
    if (p->owned && p->data_chunk) {
        duckdb_destroy_data_chunk(&(p->data_chunk));
    }

    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBDataChunk *ctx;
    return TypedData_Make_Struct(klass, rubyDuckDBDataChunk, &data_chunk_data_type, ctx);
}

static size_t memsize(const void *p) {
    const rubyDuckDBDataChunk *ctx = p;
    return sizeof(rubyDuckDBDataChunk) + ctx->column_types_count * sizeof(duckdb_logical_type);
}

rubyDuckDBDataChunk *rbduckdb_get_struct_data_chunk(VALUE obj) {
    rubyDuckDBDataChunk *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBDataChunk, &data_chunk_data_type, ctx);
    return ctx;
}

VALUE rbduckdb_create_data_chunk(duckdb_data_chunk chunk, bool owned) {
    VALUE obj = allocate(cDuckDBDataChunk);
    rubyDuckDBDataChunk *ctx;

    TypedData_Get_Struct(obj, rubyDuckDBDataChunk, &data_chunk_data_type, ctx);
    ctx->data_chunk = chunk;
    ctx->owned = owned;
    return obj;
}

static VALUE data_chunk_initialize(int argc, VALUE *argv, VALUE self) {
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

    free_column_types(ctx);
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
static VALUE data_chunk_column_count(VALUE self) {
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
static VALUE data_chunk_size(VALUE self) {
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
static VALUE data_chunk_set_size(VALUE self, VALUE size) {
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
static VALUE data_chunk_get_vector(VALUE self, VALUE col_idx) {
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
    vector_ctx = rbduckdb_get_struct_vector(vector_obj);
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
static VALUE data_chunk__reset(VALUE self) {
    rubyDuckDBDataChunk *ctx;

    TypedData_Get_Struct(self, rubyDuckDBDataChunk, &data_chunk_data_type, ctx);

    duckdb_data_chunk_reset(ctx->data_chunk);

    return self;
}

static duckdb_logical_type column_type(rubyDuckDBDataChunk *ctx, idx_t col_idx) {
    idx_t column_count;

    if (ctx->column_types == NULL) {
        column_count = duckdb_data_chunk_get_column_count(ctx->data_chunk);
        ctx->column_types = ALLOC_N(duckdb_logical_type, column_count);
        MEMZERO(ctx->column_types, duckdb_logical_type, column_count);
        ctx->column_types_count = column_count;
    }
    if (ctx->column_types[col_idx] == NULL) {
        ctx->column_types[col_idx] = duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(ctx->data_chunk, col_idx));
    }
    return ctx->column_types[col_idx];
}

/*
 * call-seq:
 *   data_chunk.set_value(col_idx, row_idx, value) -> value
 *
 * Sets a value at the specified column and row index.
 * The value is converted according to the column's logical type, the
 * same way a ScalarFunction return value is converted. +nil+ sets NULL.
 *
 *   output.set_value(0, 0, 42)       # column 0, row 0
 *   output.set_value(1, 0, 'Alice')  # column 1, row 0
 *   output.set_value(0, 1, nil)      # NULL
 *
 * Raises IndexError if +col_idx+ or +row_idx+ is out of range, and
 * ArgumentError if the column type is not supported.
 */
static VALUE data_chunk_set_value(VALUE self, VALUE col_idx, VALUE row_idx, VALUE value) {
    rubyDuckDBDataChunk *ctx;
    idx_t col;
    idx_t row;
    duckdb_vector vector;
    duckdb_logical_type type;

    TypedData_Get_Struct(self, rubyDuckDBDataChunk, &data_chunk_data_type, ctx);

    if (!ctx->data_chunk) {
        rb_raise(eDuckDBError, "data chunk is not initialized");
    }
    if (NUM2LL(col_idx) < 0) {
        rb_raise(rb_eIndexError, "column index %"PRIsVALUE" out of range", rb_inspect(col_idx));
    }
    if (NUM2LL(row_idx) < 0) {
        rb_raise(rb_eIndexError, "row index %"PRIsVALUE" out of range", rb_inspect(row_idx));
    }
    col = NUM2ULL(col_idx);
    row = NUM2ULL(row_idx);
    if (col >= duckdb_data_chunk_get_column_count(ctx->data_chunk)) {
        rb_raise(rb_eIndexError, "column index %llu out of range", (unsigned long long)col);
    }
    if (row >= duckdb_vector_size()) {
        rb_raise(rb_eIndexError, "row index %llu out of range (vector size is %llu)",
                 (unsigned long long)row, (unsigned long long)duckdb_vector_size());
    }

    vector = duckdb_data_chunk_get_vector(ctx->data_chunk, col);
    type = column_type(ctx, col);
    rbduckdb_vector_set_value_at(vector, type, row, value);

    return value;
}

void rbduckdb_init_data_chunk(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBDataChunk = rb_define_class_under(mDuckDB, "DataChunk", rb_cObject);
    rb_define_alloc_func(cDuckDBDataChunk, allocate);

    rb_define_method(cDuckDBDataChunk, "initialize", data_chunk_initialize, -1);
    rb_define_method(cDuckDBDataChunk, "column_count", data_chunk_column_count, 0);
    rb_define_method(cDuckDBDataChunk, "size", data_chunk_size, 0);
    rb_define_method(cDuckDBDataChunk, "size=", data_chunk_set_size, 1);
    rb_define_method(cDuckDBDataChunk, "get_vector", data_chunk_get_vector, 1);
    rb_define_method(cDuckDBDataChunk, "set_value", data_chunk_set_value, 3);
    rb_define_private_method(cDuckDBDataChunk, "_reset", data_chunk__reset, 0);
}
