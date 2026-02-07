#include "ruby-duckdb.h"

VALUE cDuckDBScalarFunction;

static void mark(void *);
static void deallocate(void *);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE duckdb_scalar_function_initialize(VALUE self);
static VALUE rbduckdb_scalar_function_set_name(VALUE self, VALUE name);
static VALUE rbduckdb_scalar_function__set_return_type(VALUE self, VALUE logical_type);
static VALUE rbduckdb_scalar_function_add_parameter(VALUE self, VALUE logical_type);
static VALUE rbduckdb_scalar_function_set_function(VALUE self);
static void scalar_function_callback(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output);

static const rb_data_type_t scalar_function_data_type = {
    "DuckDB/ScalarFunction",
    {mark, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void mark(void *ctx) {
    rubyDuckDBScalarFunction *p = (rubyDuckDBScalarFunction *)ctx;
    rb_gc_mark(p->function_proc);
}

static void deallocate(void * ctx) {
    rubyDuckDBScalarFunction *p = (rubyDuckDBScalarFunction *)ctx;
    duckdb_destroy_scalar_function(&(p->scalar_function));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBScalarFunction *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBScalarFunction));
    return TypedData_Wrap_Struct(klass, &scalar_function_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBScalarFunction);
}

static VALUE duckdb_scalar_function_initialize(VALUE self) {
    rubyDuckDBScalarFunction *p;
    TypedData_Get_Struct(self, rubyDuckDBScalarFunction, &scalar_function_data_type, p);
    p->scalar_function = duckdb_create_scalar_function();
    p->function_proc = Qnil;
    return self;
}

static VALUE rbduckdb_scalar_function_set_name(VALUE self, VALUE name) {
    rubyDuckDBScalarFunction *p;
    TypedData_Get_Struct(self, rubyDuckDBScalarFunction, &scalar_function_data_type, p);

    const char *str = StringValuePtr(name);
    duckdb_scalar_function_set_name(p->scalar_function, str);

    return self;
}

static VALUE rbduckdb_scalar_function__set_return_type(VALUE self, VALUE logical_type) {
    rubyDuckDBScalarFunction *p;
    rubyDuckDBLogicalType *lt;

    TypedData_Get_Struct(self, rubyDuckDBScalarFunction, &scalar_function_data_type, p);
    lt = get_struct_logical_type(logical_type);

    duckdb_scalar_function_set_return_type(p->scalar_function, lt->logical_type);

    return self;
}

static VALUE rbduckdb_scalar_function_add_parameter(VALUE self, VALUE logical_type) {
    rubyDuckDBScalarFunction *p;
    rubyDuckDBLogicalType *lt;

    TypedData_Get_Struct(self, rubyDuckDBScalarFunction, &scalar_function_data_type, p);
    lt = get_struct_logical_type(logical_type);

    duckdb_scalar_function_add_parameter(p->scalar_function, lt->logical_type);

    return self;
}

static void scalar_function_callback(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    rubyDuckDBScalarFunction *ctx;
    VALUE result;
    idx_t row_count;
    idx_t col_count;
    idx_t i, j;
    uint64_t *output_validity;
    duckdb_vector *input_vectors;
    duckdb_logical_type *input_types;
    VALUE *args;

    ctx = (rubyDuckDBScalarFunction *)duckdb_scalar_function_get_extra_info(info);

    if (ctx == NULL || ctx->function_proc == Qnil) {
        duckdb_vector_ensure_validity_writable(output);
        return;
    }

    // Get the number of rows and columns
    row_count = duckdb_data_chunk_get_size(input);
    col_count = duckdb_data_chunk_get_column_count(input);

    // Get output vector data (INTEGER type uses int32_t)
    int32_t *output_data_int32 = (int32_t *)duckdb_vector_get_data(output);

    // If no parameters, call block once and replicate result to all rows
    if (col_count == 0) {
        int32_t *output_data_int32 = (int32_t *)duckdb_vector_get_data(output);
        result = rb_funcall(ctx->function_proc, rb_intern("call"), 0);

        if (result == Qnil) {
            duckdb_vector_ensure_validity_writable(output);
            output_validity = duckdb_vector_get_validity(output);
            for (i = 0; i < row_count; i++) {
                duckdb_validity_set_row_invalid(output_validity, i);
            }
        } else {
            for (i = 0; i < row_count; i++) {
                output_data_int32[i] = NUM2INT(result);
            }
        }
        return;
    }

    // Allocate arrays to hold input vectors and their types
    input_vectors = ALLOC_N(duckdb_vector, col_count);
    input_types = ALLOC_N(duckdb_logical_type, col_count);
    args = ALLOC_N(VALUE, col_count);

    // Get all input vectors and their types
    for (j = 0; j < col_count; j++) {
        input_vectors[j] = duckdb_data_chunk_get_vector(input, j);
        input_types[j] = duckdb_vector_get_column_type(input_vectors[j]);
    }

    // Ensure output validity is writable (needed if any row might return NULL)
    duckdb_vector_ensure_validity_writable(output);
    output_validity = duckdb_vector_get_validity(output);

    // Process each row
    for (i = 0; i < row_count; i++) {
        // Build arguments array for this row using vector_value_at
        for (j = 0; j < col_count; j++) {
            args[j] = rbduckdb_vector_value_at(input_vectors[j], input_types[j], i);
        }

        // Call the Ruby block with the arguments
        result = rb_funcallv(ctx->function_proc, rb_intern("call"), col_count, args);

        // Write result to output
        if (result == Qnil) {
            duckdb_validity_set_row_invalid(output_validity, i);
        } else {
            output_data_int32[i] = NUM2INT(result);
        }
    }

    // Free allocated memory
    xfree(args);
    xfree(input_types);
    xfree(input_vectors);
}

rubyDuckDBScalarFunction *get_struct_scalar_function(VALUE obj) {
    rubyDuckDBScalarFunction *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBScalarFunction, &scalar_function_data_type, ctx);
    return ctx;
}

/* :nodoc: */
static VALUE rbduckdb_scalar_function_set_function(VALUE self) {
    rubyDuckDBScalarFunction *p;

    if (!rb_block_given_p()) {
        rb_raise(rb_eArgError, "block is required");
    }

    TypedData_Get_Struct(self, rubyDuckDBScalarFunction, &scalar_function_data_type, p);

    p->function_proc = rb_block_proc();

    duckdb_scalar_function_set_extra_info(p->scalar_function, p, NULL);
    duckdb_scalar_function_set_function(p->scalar_function, scalar_function_callback);

    // Mark as volatile to prevent constant folding during query optimization
    // This prevents DuckDB from evaluating the function at planning time.
    // NOTE: Ruby scalar functions require single-threaded execution (PRAGMA threads=1)
    // because Ruby proc callbacks cannot be safely invoked from DuckDB worker threads.
    duckdb_scalar_function_set_volatile(p->scalar_function);

    return self;
}

void rbduckdb_init_duckdb_scalar_function(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBScalarFunction = rb_define_class_under(mDuckDB, "ScalarFunction", rb_cObject);
    rb_define_alloc_func(cDuckDBScalarFunction, allocate);
    rb_define_method(cDuckDBScalarFunction, "initialize", duckdb_scalar_function_initialize, 0);
    rb_define_method(cDuckDBScalarFunction, "set_name", rbduckdb_scalar_function_set_name, 1);
    rb_define_method(cDuckDBScalarFunction, "name=", rbduckdb_scalar_function_set_name, 1);
    rb_define_private_method(cDuckDBScalarFunction, "_set_return_type", rbduckdb_scalar_function__set_return_type, 1);
    rb_define_method(cDuckDBScalarFunction, "add_parameter", rbduckdb_scalar_function_add_parameter, 1);
    rb_define_method(cDuckDBScalarFunction, "set_function", rbduckdb_scalar_function_set_function, 0);
}
