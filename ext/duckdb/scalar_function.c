#include "ruby-duckdb.h"

VALUE cDuckDBScalarFunction;

static void mark(void *);
static void deallocate(void *);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static void compact(void *);
static VALUE duckdb_scalar_function_initialize(VALUE self);
static VALUE rbduckdb_scalar_function_set_name(VALUE self, VALUE name);
static VALUE rbduckdb_scalar_function__set_return_type(VALUE self, VALUE logical_type);
static VALUE rbduckdb_scalar_function__set_varargs(VALUE self, VALUE logical_type);
static VALUE rbduckdb_scalar_function_add_parameter(VALUE self, VALUE logical_type);
static VALUE rbduckdb_scalar_function__set_special_handling(VALUE self);
static VALUE rbduckdb_scalar_function_set_function(VALUE self);
static VALUE rbduckdb_scalar_function__set_bind(VALUE self);
static void scalar_function_callback(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output);
static void scalar_function_bind_callback(duckdb_bind_info info);


struct callback_arg {
    rubyDuckDBScalarFunction *ctx;
    duckdb_function_info info;
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

/* Execute a callback (called with GVL held) */
static VALUE execute_callback(VALUE varg) {
    struct callback_arg *arg = (struct callback_arg *)varg;

    if (arg->col_count == 0) {
        rb_ensure(process_no_param_rows, (VALUE)arg, cleanup_callback, (VALUE)arg);
    } else {
        rb_ensure(process_rows, (VALUE)arg, cleanup_callback, (VALUE)arg);
    }

    return Qnil;
}

/*
 * Execute a callback with rb_protect and report any Ruby exception
 * to DuckDB via duckdb_scalar_function_set_error.
 *
 * This is the scalar-function-specific wrapper around the shared
 * function_executor dispatcher: it handles Ruby exceptions by routing
 * them to the DuckDB-side error reporter.
 */
static void execute_callback_protected(void *user_data) {
    struct callback_arg *arg = (struct callback_arg *)user_data;
    int exception_state;

    rb_protect(execute_callback, (VALUE)arg, &exception_state);
    if (exception_state) {
        VALUE errinfo = rb_errinfo();
        if (errinfo != Qnil) {
            VALUE msg = rb_funcall(errinfo, rb_intern("message"), 0);
            duckdb_scalar_function_set_error(arg->info, StringValueCStr(msg));
        }
        rb_set_errinfo(Qnil);
    }
}

static const rb_data_type_t scalar_function_data_type = {
    "DuckDB/ScalarFunction",
    {mark, deallocate, memsize, compact},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void mark(void *ctx) {
    rubyDuckDBScalarFunction *p = (rubyDuckDBScalarFunction *)ctx;
    rb_gc_mark(p->function_proc);
    rb_gc_mark(p->bind_proc);
}

static void deallocate(void * ctx) {
    rubyDuckDBScalarFunction *p = (rubyDuckDBScalarFunction *)ctx;
    duckdb_destroy_scalar_function(&(p->scalar_function));
    xfree(p);
}

/*
 * GC compaction callback - updates VALUE references that may have moved during compaction.
 * This is critical for Ruby 2.7+ where GC can move objects in memory.
 * Without this, the function_proc VALUE could become a stale pointer after compaction,
 * leading to crashes when DuckDB invokes the callback.
 */
static void compact(void *ctx) {
    rubyDuckDBScalarFunction *p = (rubyDuckDBScalarFunction *)ctx;
    if (p->function_proc != Qnil) {
        p->function_proc = rb_gc_location(p->function_proc);
    }
    if (p->bind_proc != Qnil) {
        p->bind_proc = rb_gc_location(p->bind_proc);
    }
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
    p->bind_proc = Qnil;
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

static VALUE rbduckdb_scalar_function__set_varargs(VALUE self, VALUE logical_type) {
    rubyDuckDBScalarFunction *p;
    rubyDuckDBLogicalType *lt;

    TypedData_Get_Struct(self, rubyDuckDBScalarFunction, &scalar_function_data_type, p);
    lt = get_struct_logical_type(logical_type);

    duckdb_scalar_function_set_varargs(p->scalar_function, lt->logical_type);

    return self;
}

static VALUE rbduckdb_scalar_function__set_special_handling(VALUE self) {
    rubyDuckDBScalarFunction *p;

    TypedData_Get_Struct(self, rubyDuckDBScalarFunction, &scalar_function_data_type, p);
    duckdb_scalar_function_set_special_handling(p->scalar_function);

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

/*
 * The DuckDB callback entry point.
 *
 * Builds the per-invocation callback_arg and hands control to the shared
 * function_executor dispatcher. The dispatcher automatically selects the
 * right path (direct call / rb_thread_call_with_gvl / executor thread)
 * depending on the calling thread's state.
 */
static void scalar_function_callback(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    rubyDuckDBScalarFunction *ctx;
    idx_t i;
    struct callback_arg arg;

    ctx = (rubyDuckDBScalarFunction *)duckdb_scalar_function_get_extra_info(info);

    if (ctx == NULL || ctx->function_proc == Qnil) {
        /* Mark all rows as NULL to avoid returning uninitialized data */
        idx_t row_count = duckdb_data_chunk_get_size(input);
        uint64_t *validity;
        duckdb_vector_ensure_validity_writable(output);
        validity = duckdb_vector_get_validity(output);
        for (i = 0; i < row_count; i++) {
            duckdb_validity_set_row_invalid(validity, i);
        }
        return;
    }

    /* Initialize callback argument structure */
    arg.ctx = ctx;
    arg.info = info;
    arg.input = input;
    arg.output = output;
    arg.output_type = duckdb_vector_get_column_type(output);
    arg.input_vectors = NULL;
    arg.input_types = NULL;
    arg.args = NULL;
    arg.row_count = duckdb_data_chunk_get_size(input);
    arg.col_count = duckdb_data_chunk_get_column_count(input);

    rbduckdb_function_executor_dispatch(execute_callback_protected, &arg);
}

static VALUE process_no_param_rows(VALUE varg) {
    struct callback_arg *arg = (struct callback_arg *)varg;
    idx_t i;
    VALUE result;

    result = rb_funcall(arg->ctx->function_proc, rb_intern("call"), 0);

    for (i = 0; i < arg->row_count; i++) {
        rbduckdb_vector_set_value_at(arg->output, arg->output_type, i, result);
    }

    return Qnil;
}

static VALUE process_rows(VALUE varg) {
    struct callback_arg *arg = (struct callback_arg *)varg;
    idx_t i, j;
    VALUE result;

    /* Allocate arrays to hold input vectors and their types */
    arg->input_vectors = ALLOC_N(duckdb_vector, arg->col_count);
    arg->input_types = ALLOC_N(duckdb_logical_type, arg->col_count);
    arg->args = ALLOC_N(VALUE, arg->col_count);

    /* Get all input vectors and their types */
    for (j = 0; j < arg->col_count; j++) {
        arg->input_vectors[j] = duckdb_data_chunk_get_vector(arg->input, j);
        arg->input_types[j] = duckdb_vector_get_column_type(arg->input_vectors[j]);
    }

    /* Process each row */
    for (i = 0; i < arg->row_count; i++) {
        /* Build arguments array for this row using vector_value_at */
        for (j = 0; j < arg->col_count; j++) {
            arg->args[j] = rbduckdb_vector_value_at(arg->input_vectors[j], arg->input_types[j], i);
        }

        /* Call the Ruby block with the arguments */
        result = rb_funcallv(arg->ctx->function_proc, rb_intern("call"), arg->col_count, arg->args);

        /* Write result to output using helper function */
        rbduckdb_vector_set_value_at(arg->output, arg->output_type, i, result);
    }

    return Qnil;
}

static VALUE cleanup_callback(VALUE varg) {
    struct callback_arg *arg = (struct callback_arg *)varg;
    idx_t j;

    /* Destroy all logical types */
    if (arg->input_types != NULL) {
        for (j = 0; j < arg->col_count; j++) {
            duckdb_destroy_logical_type(&arg->input_types[j]);
        }
    }
    duckdb_destroy_logical_type(&arg->output_type);

    /* Free allocated memory */
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

    /*
     * Mark as volatile to prevent constant folding during query optimization.
     * This prevents DuckDB from evaluating the function at planning time.
     */
    duckdb_scalar_function_set_volatile(p->scalar_function);

    /* Ensure the global executor thread is running for multi-thread dispatch */
    rbduckdb_function_executor_ensure_started();

    return self;
}

/* :nodoc: */
static VALUE rbduckdb_scalar_function__set_bind(VALUE self) {
    rubyDuckDBScalarFunction *p;

    TypedData_Get_Struct(self, rubyDuckDBScalarFunction, &scalar_function_data_type, p);

    p->bind_proc = rb_block_proc();

    duckdb_scalar_function_set_extra_info(p->scalar_function, p, NULL);
    duckdb_scalar_function_set_bind(p->scalar_function, scalar_function_bind_callback);

    return self;
}

struct bind_call_arg {
    VALUE bind_proc;
    VALUE bind_info_obj;
};

static VALUE call_bind_proc(VALUE varg) {
    struct bind_call_arg *arg = (struct bind_call_arg *)varg;
    return rb_funcall(arg->bind_proc, rb_intern("call"), 1, arg->bind_info_obj);
}

/*
 * The bind callback: called once at query planning time.
 *
 * Retrieves the rubyDuckDBScalarFunction context via extra_info, creates a
 * ScalarFunction::BindInfo Ruby object wrapping the duckdb_bind_info, and
 * calls the stored bind_proc with it.
 *
 * The bind callback is invoked from the calling Ruby thread during planning,
 * so we call rb_protect directly. For a Ruby thread that has released the GVL
 * we reacquire it via rb_thread_call_with_gvl.
 */
static void scalar_function_bind_callback(duckdb_bind_info info) {
    rubyDuckDBScalarFunction *ctx;
    int exception_state;
    struct bind_call_arg arg;

    ctx = (rubyDuckDBScalarFunction *)duckdb_scalar_function_bind_get_extra_info(info);
    if (ctx == NULL || ctx->bind_proc == Qnil) return;

    arg.bind_proc = ctx->bind_proc;
    arg.bind_info_obj = rbduckdb_scalar_function_bind_info_new(info);

    rb_protect(call_bind_proc, (VALUE)&arg, &exception_state);
    if (exception_state) {
        VALUE errinfo = rb_errinfo();
        if (errinfo != Qnil) {
            VALUE msg = rb_funcall(errinfo, rb_intern("message"), 0);
            duckdb_scalar_function_bind_set_error(info, StringValueCStr(msg));
        }
        rb_set_errinfo(Qnil);
    }
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
    rb_define_private_method(cDuckDBScalarFunction, "_set_varargs", rbduckdb_scalar_function__set_varargs, 1);
    rb_define_private_method(cDuckDBScalarFunction, "_set_special_handling", rbduckdb_scalar_function__set_special_handling, 0);
    rb_define_private_method(cDuckDBScalarFunction, "_add_parameter", rbduckdb_scalar_function_add_parameter, 1);
    rb_define_method(cDuckDBScalarFunction, "set_function", rbduckdb_scalar_function_set_function, 0);
    rb_define_private_method(cDuckDBScalarFunction, "_set_bind", rbduckdb_scalar_function__set_bind, 0);
}
