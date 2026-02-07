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
static void vector_set_value_at(duckdb_vector vector, duckdb_logical_type element_type, idx_t index, VALUE value);

struct callback_arg {
    rubyDuckDBScalarFunction *ctx;
    duckdb_data_chunk input;
    duckdb_vector output;
    duckdb_logical_type output_type;
    duckdb_vector *input_vectors;
    duckdb_logical_type *input_types;
    VALUE *args;
    idx_t row_count;
    idx_t col_count;
};

static VALUE process_rows(VALUE arg);
static VALUE process_no_param_rows(VALUE arg);
static VALUE cleanup_callback(VALUE arg);

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
    idx_t i;
    struct callback_arg arg;

    ctx = (rubyDuckDBScalarFunction *)duckdb_scalar_function_get_extra_info(info);

    if (ctx == NULL || ctx->function_proc == Qnil) {
        // Mark all rows as NULL to avoid returning uninitialized data
        idx_t row_count = duckdb_data_chunk_get_size(input);
        uint64_t *validity;
        duckdb_vector_ensure_validity_writable(output);
        validity = duckdb_vector_get_validity(output);
        for (i = 0; i < row_count; i++) {
            duckdb_validity_set_row_invalid(validity, i);
        }
        return;
    }

    // Initialize callback argument structure
    arg.ctx = ctx;
    arg.input = input;
    arg.output = output;
    arg.output_type = duckdb_vector_get_column_type(output);
    arg.input_vectors = NULL;
    arg.input_types = NULL;
    arg.args = NULL;
    arg.row_count = duckdb_data_chunk_get_size(input);
    arg.col_count = duckdb_data_chunk_get_column_count(input);

    // If no parameters, call block once and replicate result to all rows
    if (arg.col_count == 0) {
        rb_ensure(process_no_param_rows, (VALUE)&arg, cleanup_callback, (VALUE)&arg);
        return;
    }

    // Process rows with proper cleanup on exception
    rb_ensure(process_rows, (VALUE)&arg, cleanup_callback, (VALUE)&arg);
}

static VALUE process_no_param_rows(VALUE varg) {
    struct callback_arg *arg = (struct callback_arg *)varg;
    idx_t i;
    VALUE result;

    result = rb_funcall(arg->ctx->function_proc, rb_intern("call"), 0);

    for (i = 0; i < arg->row_count; i++) {
        vector_set_value_at(arg->output, arg->output_type, i, result);
    }

    return Qnil;
}

static VALUE process_rows(VALUE varg) {
    struct callback_arg *arg = (struct callback_arg *)varg;
    idx_t i, j;
    VALUE result;

    // Allocate arrays to hold input vectors and their types
    arg->input_vectors = ALLOC_N(duckdb_vector, arg->col_count);
    arg->input_types = ALLOC_N(duckdb_logical_type, arg->col_count);
    arg->args = ALLOC_N(VALUE, arg->col_count);

    // Get all input vectors and their types
    for (j = 0; j < arg->col_count; j++) {
        arg->input_vectors[j] = duckdb_data_chunk_get_vector(arg->input, j);
        arg->input_types[j] = duckdb_vector_get_column_type(arg->input_vectors[j]);
    }

    // Process each row
    for (i = 0; i < arg->row_count; i++) {
        // Build arguments array for this row using vector_value_at
        for (j = 0; j < arg->col_count; j++) {
            arg->args[j] = rbduckdb_vector_value_at(arg->input_vectors[j], arg->input_types[j], i);
        }

        // Call the Ruby block with the arguments
        result = rb_funcallv(arg->ctx->function_proc, rb_intern("call"), arg->col_count, arg->args);

        // Write result to output using helper function
        vector_set_value_at(arg->output, arg->output_type, i, result);
    }

    return Qnil;
}

static VALUE cleanup_callback(VALUE varg) {
    struct callback_arg *arg = (struct callback_arg *)varg;
    idx_t j;

    // Destroy all logical types
    if (arg->input_types != NULL) {
        for (j = 0; j < arg->col_count; j++) {
            duckdb_destroy_logical_type(&arg->input_types[j]);
        }
    }
    duckdb_destroy_logical_type(&arg->output_type);

    // Free allocated memory
    if (arg->args != NULL) {
        xfree(arg->args);
    }
    if (arg->input_types != NULL) {
        xfree(arg->input_types);
    }
    if (arg->input_vectors != NULL) {
        xfree(arg->input_vectors);
    }

    return Qnil;
}

static void vector_set_value_at(duckdb_vector vector, duckdb_logical_type element_type, idx_t index, VALUE value) {
    duckdb_type type_id;
    void* vector_data;
    uint64_t *validity;

    // Handle NULL values
    if (value == Qnil) {
        duckdb_vector_ensure_validity_writable(vector);
        validity = duckdb_vector_get_validity(vector);
        duckdb_validity_set_row_invalid(validity, index);
        return;
    }

    type_id = duckdb_get_type_id(element_type);
    vector_data = duckdb_vector_get_data(vector);

    switch(type_id) {
        case DUCKDB_TYPE_BOOLEAN:
            ((bool *)vector_data)[index] = RTEST(value);
            break;
        case DUCKDB_TYPE_INTEGER:
            ((int32_t *)vector_data)[index] = NUM2INT(value);
            break;
        case DUCKDB_TYPE_BIGINT:
            ((int64_t *)vector_data)[index] = NUM2LL(value);
            break;
        case DUCKDB_TYPE_FLOAT:
            ((float *)vector_data)[index] = (float)NUM2DBL(value);
            break;
        case DUCKDB_TYPE_DOUBLE:
            ((double *)vector_data)[index] = NUM2DBL(value);
            break;
        case DUCKDB_TYPE_VARCHAR: {
            // VARCHAR requires special API, not direct array assignment
            VALUE str = rb_obj_as_string(value);
            const char *str_ptr = StringValuePtr(str);
            idx_t str_len = RSTRING_LEN(str);
            duckdb_vector_assign_string_element_len(vector, index, str_ptr, str_len);
            break;
        }
        case DUCKDB_TYPE_BLOB: {
            // BLOB uses same API as VARCHAR, but expects binary data
            VALUE str = rb_obj_as_string(value);
            const char *str_ptr = StringValuePtr(str);
            idx_t str_len = RSTRING_LEN(str);
            duckdb_vector_assign_string_element_len(vector, index, str_ptr, str_len);
            break;
        }
        case DUCKDB_TYPE_TIMESTAMP: {
            // Convert Ruby Time to DuckDB timestamp (microseconds since epoch)
            if (!rb_obj_is_kind_of(value, rb_cTime)) {
                rb_raise(rb_eTypeError, "Expected Time object for TIMESTAMP");
            }
            
            duckdb_timestamp_struct ts_struct;
            ts_struct.date.year = FIX2INT(rb_funcall(value, rb_intern("year"), 0));
            ts_struct.date.month = FIX2INT(rb_funcall(value, rb_intern("month"), 0));
            ts_struct.date.day = FIX2INT(rb_funcall(value, rb_intern("day"), 0));
            ts_struct.time.hour = FIX2INT(rb_funcall(value, rb_intern("hour"), 0));
            ts_struct.time.min = FIX2INT(rb_funcall(value, rb_intern("min"), 0));
            ts_struct.time.sec = FIX2INT(rb_funcall(value, rb_intern("sec"), 0));
            ts_struct.time.micros = FIX2INT(rb_funcall(value, rb_intern("usec"), 0));
            
            duckdb_timestamp ts = duckdb_to_timestamp(ts_struct);
            ((duckdb_timestamp *)vector_data)[index] = ts;
            break;
        }
        default:
            rb_raise(rb_eArgError, "Unsupported return type for scalar function");
            break;
    }
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
