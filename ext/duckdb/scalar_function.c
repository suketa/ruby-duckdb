#include "ruby-duckdb.h"
#include <pthread.h>

/*
 * Thread detection functions (available since Ruby 2.3).
 * Used to determine the correct dispatch path for scalar function callbacks.
 */
extern int ruby_thread_has_gvl_p(void);
extern int ruby_native_thread_p(void);

VALUE cDuckDBScalarFunction;

static void mark(void *);
static void deallocate(void *);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static void compact(void *);
static VALUE duckdb_scalar_function_initialize(VALUE self);
static VALUE rbduckdb_scalar_function_set_name(VALUE self, VALUE name);
static VALUE rbduckdb_scalar_function__set_return_type(VALUE self, VALUE logical_type);
static VALUE rbduckdb_scalar_function_add_parameter(VALUE self, VALUE logical_type);
static VALUE rbduckdb_scalar_function_set_function(VALUE self);
static void scalar_function_callback(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output);
static void vector_set_value_at(duckdb_vector vector, duckdb_logical_type element_type, idx_t index, VALUE value);

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

/*
 * ============================================================================
 * Global Executor Thread
 * ============================================================================
 *
 * DuckDB calls scalar function callbacks from its own worker threads, which
 * are NOT Ruby threads. Ruby's GVL (Global VM Lock) cannot be acquired from
 * non-Ruby threads (rb_thread_call_with_gvl crashes with rb_bug).
 *
 * Solution (modeled after FFI gem's async callback dispatcher):
 * - A global Ruby "executor" thread waits for callback requests.
 * - DuckDB worker threads enqueue requests via pthread mutex/condvar and block.
 * - The executor thread processes callbacks with the GVL, then signals completion.
 *
 * When the callback is invoked from a Ruby thread (e.g., threads=1 where DuckDB
 * uses the calling thread), we use rb_thread_call_with_gvl directly, avoiding
 * the executor overhead.
 */

/* Per-callback request, stack-allocated on the DuckDB worker thread */
struct callback_request {
    struct callback_arg *cb_arg;
    int done;
    pthread_mutex_t done_mutex;
    pthread_cond_t done_cond;
    struct callback_request *next;
};

/* Global executor state */
static pthread_mutex_t g_executor_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_executor_cond = PTHREAD_COND_INITIALIZER;
static struct callback_request *g_request_list = NULL;
static VALUE g_executor_thread = Qnil;
static int g_executor_started = 0;

/* Data passed to the executor wait function */
struct executor_wait_data {
    struct callback_request *request;
    int stop;
};

/* Runs without GVL: blocks on condvar waiting for a callback request */
static void *executor_wait_func(void *data) {
    struct executor_wait_data *w = (struct executor_wait_data *)data;

    w->request = NULL;

    pthread_mutex_lock(&g_executor_mutex);
    while (!w->stop && g_request_list == NULL) {
        pthread_cond_wait(&g_executor_cond, &g_executor_mutex);
    }
    if (g_request_list != NULL) {
        w->request = g_request_list;
        g_request_list = g_request_list->next;
    }
    pthread_mutex_unlock(&g_executor_mutex);

    return NULL;
}

/* Unblock function: called by Ruby to interrupt the executor (e.g., VM shutdown) */
static void executor_stop_func(void *data) {
    struct executor_wait_data *w = (struct executor_wait_data *)data;

    pthread_mutex_lock(&g_executor_mutex);
    w->stop = 1;
    pthread_cond_signal(&g_executor_cond);
    pthread_mutex_unlock(&g_executor_mutex);
}

/* Execute a callback with exception protection (called with GVL held) */
static VALUE execute_callback_protected(VALUE varg) {
    struct callback_arg *arg = (struct callback_arg *)varg;

    if (arg->col_count == 0) {
        rb_ensure(process_no_param_rows, (VALUE)arg, cleanup_callback, (VALUE)arg);
    } else {
        rb_ensure(process_rows, (VALUE)arg, cleanup_callback, (VALUE)arg);
    }

    return Qnil;
}

/* The executor thread main loop (Ruby thread) */
static VALUE executor_thread_func(void *data) {
    struct executor_wait_data w;
    w.stop = 0;

    while (!w.stop) {
        /* Release GVL and wait for a callback request */
        rb_thread_call_without_gvl(executor_wait_func, &w, executor_stop_func, &w);

        if (w.request != NULL) {
            struct callback_request *req = w.request;
            struct callback_arg *arg = req->cb_arg;
            int exception_state;

            /* Execute the Ruby callback with the GVL */
            rb_protect(execute_callback_protected, (VALUE)arg, &exception_state);
            if (exception_state) {
                /* Report error to DuckDB and clear the Ruby exception */
                VALUE errinfo = rb_errinfo();
                if (errinfo != Qnil) {
                    VALUE msg = rb_funcall(errinfo, rb_intern("message"), 0);
                    duckdb_scalar_function_set_error(arg->info, StringValueCStr(msg));
                }
                rb_set_errinfo(Qnil);
            }

            /* Signal the DuckDB worker thread that the callback is done */
            pthread_mutex_lock(&req->done_mutex);
            req->done = 1;
            pthread_cond_signal(&req->done_cond);
            pthread_mutex_unlock(&req->done_mutex);
        }
    }

    return Qnil;
}

/* Start the global executor thread (must be called from a Ruby thread) */
static void ensure_executor_started(void) {
    if (g_executor_started) return;

    g_executor_thread = rb_thread_create(executor_thread_func, NULL);
    rb_global_variable(&g_executor_thread);
    g_executor_started = 1;
}

/*
 * Dispatch a callback to the global executor thread.
 * Called from a DuckDB worker thread (non-Ruby thread).
 * The caller blocks until the callback is processed.
 */
static void dispatch_callback_to_executor(struct callback_arg *arg) {
    struct callback_request req;

    req.cb_arg = arg;
    req.done = 0;
    req.next = NULL;
    pthread_mutex_init(&req.done_mutex, NULL);
    pthread_cond_init(&req.done_cond, NULL);

    /* Enqueue the request */
    pthread_mutex_lock(&g_executor_mutex);
    req.next = g_request_list;
    g_request_list = &req;
    pthread_cond_signal(&g_executor_cond);
    pthread_mutex_unlock(&g_executor_mutex);

    /* Wait for the executor to process our callback */
    pthread_mutex_lock(&req.done_mutex);
    while (!req.done) {
        pthread_cond_wait(&req.done_cond, &req.done_mutex);
    }
    pthread_mutex_unlock(&req.done_mutex);

    pthread_cond_destroy(&req.done_cond);
    pthread_mutex_destroy(&req.done_mutex);
}

/*
 * Wrapper for rb_thread_call_with_gvl: executes the callback after
 * re-acquiring the GVL. Used when a Ruby thread (without GVL) is the caller.
 */
static void *callback_with_gvl(void *data) {
    struct callback_arg *arg = (struct callback_arg *)data;
    int exception_state;

    rb_protect(execute_callback_protected, (VALUE)arg, &exception_state);
    if (exception_state) {
        VALUE errinfo = rb_errinfo();
        if (errinfo != Qnil) {
            VALUE msg = rb_funcall(errinfo, rb_intern("message"), 0);
            duckdb_scalar_function_set_error(arg->info, StringValueCStr(msg));
        }
        rb_set_errinfo(Qnil);
    }

    return NULL;
}

/* ============================================================================
 * End of Executor Thread
 * ============================================================================ */

static const rb_data_type_t scalar_function_data_type = {
    "DuckDB/ScalarFunction",
    {mark, deallocate, memsize, compact},
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

/*
 * The DuckDB callback entry point.
 *
 * Three dispatch paths (modeled after FFI gem):
 *   1. Ruby thread WITH GVL    -> call directly
 *   2. Ruby thread WITHOUT GVL -> rb_thread_call_with_gvl
 *   3. Non-Ruby thread         -> dispatch to global executor thread
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

    if (ruby_native_thread_p()) {
        if (ruby_thread_has_gvl_p()) {
            /* Case 1: Ruby thread with GVL - call directly */
            execute_callback_protected((VALUE)&arg);
        } else {
            /* Case 2: Ruby thread without GVL - reacquire GVL */
            rb_thread_call_with_gvl(callback_with_gvl, &arg);
        }
    } else {
        /* Case 3: Non-Ruby thread - dispatch to executor */
        dispatch_callback_to_executor(&arg);
    }
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
        vector_set_value_at(arg->output, arg->output_type, i, result);
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

static void vector_set_value_at(duckdb_vector vector, duckdb_logical_type element_type, idx_t index, VALUE value) {
    duckdb_type type_id;
    void* vector_data;
    uint64_t *validity;

    /* Handle NULL values */
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
        case DUCKDB_TYPE_TINYINT:
            ((int8_t *)vector_data)[index] = (int8_t)NUM2INT(value);
            break;
        case DUCKDB_TYPE_UTINYINT:
            ((uint8_t *)vector_data)[index] = (uint8_t)NUM2UINT(value);
            break;
        case DUCKDB_TYPE_SMALLINT:
            ((int16_t *)vector_data)[index] = (int16_t)NUM2INT(value);
            break;
        case DUCKDB_TYPE_USMALLINT:
            ((uint16_t *)vector_data)[index] = (uint16_t)NUM2UINT(value);
            break;
        case DUCKDB_TYPE_INTEGER:
            ((int32_t *)vector_data)[index] = NUM2INT(value);
            break;
        case DUCKDB_TYPE_UINTEGER:
            ((uint32_t *)vector_data)[index] = (uint32_t)NUM2ULL(value);
            break;
        case DUCKDB_TYPE_BIGINT:
            ((int64_t *)vector_data)[index] = NUM2LL(value);
            break;
        case DUCKDB_TYPE_UBIGINT:
            ((uint64_t *)vector_data)[index] = NUM2ULL(value);
            break;
        case DUCKDB_TYPE_FLOAT:
            ((float *)vector_data)[index] = (float)NUM2DBL(value);
            break;
        case DUCKDB_TYPE_DOUBLE:
            ((double *)vector_data)[index] = NUM2DBL(value);
            break;
        case DUCKDB_TYPE_VARCHAR: {
            /* VARCHAR requires special API, not direct array assignment */
            VALUE str = rb_obj_as_string(value);
            const char *str_ptr = StringValuePtr(str);
            idx_t str_len = RSTRING_LEN(str);
            duckdb_vector_assign_string_element_len(vector, index, str_ptr, str_len);
            break;
        }
        case DUCKDB_TYPE_BLOB: {
            /* BLOB uses same API as VARCHAR, but expects binary data */
            VALUE str = rb_obj_as_string(value);
            const char *str_ptr = StringValuePtr(str);
            idx_t str_len = RSTRING_LEN(str);
            duckdb_vector_assign_string_element_len(vector, index, str_ptr, str_len);
            break;
        }
        case DUCKDB_TYPE_TIMESTAMP: {
            /* Convert Ruby Time to DuckDB timestamp (microseconds since epoch) */
            if (!rb_obj_is_kind_of(value, rb_cTime)) {
                rb_raise(rb_eTypeError, "Expected Time object for TIMESTAMP");
            }

            duckdb_timestamp_struct ts_struct;
            ts_struct.date.year = NUM2INT(rb_funcall(value, rb_intern("year"), 0));
            ts_struct.date.month = NUM2INT(rb_funcall(value, rb_intern("month"), 0));
            ts_struct.date.day = NUM2INT(rb_funcall(value, rb_intern("day"), 0));
            ts_struct.time.hour = NUM2INT(rb_funcall(value, rb_intern("hour"), 0));
            ts_struct.time.min = NUM2INT(rb_funcall(value, rb_intern("min"), 0));
            ts_struct.time.sec = NUM2INT(rb_funcall(value, rb_intern("sec"), 0));
            ts_struct.time.micros = NUM2INT(rb_funcall(value, rb_intern("usec"), 0));

            duckdb_timestamp ts = duckdb_to_timestamp(ts_struct);
            ((duckdb_timestamp *)vector_data)[index] = ts;
            break;
        }
        case DUCKDB_TYPE_DATE: {
            /* Convert Ruby Date to DuckDB date */
            VALUE date_class = rb_const_get(rb_cObject, rb_intern("Date"));
            if (!rb_obj_is_kind_of(value, date_class)) {
                rb_raise(rb_eTypeError, "Expected Date object for DATE");
            }

            VALUE year = rb_funcall(value, rb_intern("year"), 0);
            VALUE month = rb_funcall(value, rb_intern("month"), 0);
            VALUE day = rb_funcall(value, rb_intern("day"), 0);

            duckdb_date date = rbduckdb_to_duckdb_date_from_value(year, month, day);
            ((duckdb_date *)vector_data)[index] = date;
            break;
        }
        case DUCKDB_TYPE_TIME: {
            /* Convert Ruby Time to DuckDB time (time-of-day only) */
            if (!rb_obj_is_kind_of(value, rb_cTime)) {
                rb_raise(rb_eTypeError, "Expected Time object for TIME");
            }

            VALUE hour = rb_funcall(value, rb_intern("hour"), 0);
            VALUE min = rb_funcall(value, rb_intern("min"), 0);
            VALUE sec = rb_funcall(value, rb_intern("sec"), 0);
            VALUE usec = rb_funcall(value, rb_intern("usec"), 0);

            duckdb_time time = rbduckdb_to_duckdb_time_from_value(hour, min, sec, usec);
            ((duckdb_time *)vector_data)[index] = time;
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

    /*
     * Mark as volatile to prevent constant folding during query optimization.
     * This prevents DuckDB from evaluating the function at planning time.
     */
    duckdb_scalar_function_set_volatile(p->scalar_function);

    /* Ensure the global executor thread is running for multi-thread dispatch */
    ensure_executor_started();

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
    rb_define_private_method(cDuckDBScalarFunction, "_add_parameter", rbduckdb_scalar_function_add_parameter, 1);
    rb_define_method(cDuckDBScalarFunction, "set_function", rbduckdb_scalar_function_set_function, 0);
}
