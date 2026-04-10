#include "ruby-duckdb.h"

VALUE cDuckDBAggregateFunction;

/*
 * Global Ruby Hash used to keep aggregate state Ruby VALUEs alive during
 * aggregation. Keys are monotonic state IDs (see state_registry_key) that
 * survive DuckDB's internal memcpy of state buffers. Values are the Ruby
 * VALUE returned from the user's init_proc and later passed to
 * finalize_proc.
 *
 * Protected from GC via rb_gc_register_mark_object on init.
 */
static VALUE g_aggregate_state_registry;

/*
 * Monotonic counter for aggregate state IDs.  Each state_init_callback
 * assigns the next ID; because DuckDB memcpy's state buffers internally
 * (e.g. from a temporary allocation into the hash-table row layout), the
 * embedded ID is the only reliable way to match a state across init /
 * combine / finalize / destroy calls.
 */
static unsigned long long g_next_state_id = 0;

typedef struct {
    unsigned long long state_id;
    VALUE ruby_state;
} ruby_aggregate_state;

static void mark(void *);
static void deallocate(void *);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static void compact(void *);
static VALUE duckdb_aggregate_function_initialize(VALUE self);
static VALUE rbduckdb_aggregate_function_set_name(VALUE self, VALUE name);
static VALUE rbduckdb_aggregate_function__set_return_type(VALUE self, VALUE logical_type);
static VALUE rbduckdb_aggregate_function_add_parameter(VALUE self, VALUE logical_type);
static VALUE rbduckdb_aggregate_function_set_init(VALUE self);
static VALUE rbduckdb_aggregate_function_set_update(VALUE self);
static VALUE rbduckdb_aggregate_function_set_combine(VALUE self);
static VALUE rbduckdb_aggregate_function_set_finalize(VALUE self);
static VALUE rbduckdb_aggregate_function_set_special_handling(VALUE self);

static const rb_data_type_t aggregate_function_data_type = {
    "DuckDB/AggregateFunction",
    {mark, deallocate, memsize, compact},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void mark(void *ctx) {
    rubyDuckDBAggregateFunction *p = (rubyDuckDBAggregateFunction *)ctx;
    rb_gc_mark_movable(p->init_proc);
    rb_gc_mark_movable(p->update_proc);
    rb_gc_mark_movable(p->combine_proc);
    rb_gc_mark_movable(p->finalize_proc);
}

static void deallocate(void *ctx) {
    rubyDuckDBAggregateFunction *p = (rubyDuckDBAggregateFunction *)ctx;
    duckdb_destroy_aggregate_function(&(p->aggregate_function));
    xfree(p);
}

static void compact(void *ctx) {
    rubyDuckDBAggregateFunction *p = (rubyDuckDBAggregateFunction *)ctx;
    if (p->init_proc != Qnil) {
        p->init_proc = rb_gc_location(p->init_proc);
    }
    if (p->update_proc != Qnil) {
        p->update_proc = rb_gc_location(p->update_proc);
    }
    if (p->combine_proc != Qnil) {
        p->combine_proc = rb_gc_location(p->combine_proc);
    }
    if (p->finalize_proc != Qnil) {
        p->finalize_proc = rb_gc_location(p->finalize_proc);
    }
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBAggregateFunction *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBAggregateFunction));
    return TypedData_Wrap_Struct(klass, &aggregate_function_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBAggregateFunction);
}

rubyDuckDBAggregateFunction *get_struct_aggregate_function(VALUE obj) {
    rubyDuckDBAggregateFunction *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBAggregateFunction, &aggregate_function_data_type, ctx);
    return ctx;
}

static VALUE duckdb_aggregate_function_initialize(VALUE self) {
    rubyDuckDBAggregateFunction *p;
    TypedData_Get_Struct(self, rubyDuckDBAggregateFunction, &aggregate_function_data_type, p);
    p->aggregate_function = duckdb_create_aggregate_function();
    p->init_proc = Qnil;
    p->update_proc = Qnil;
    p->combine_proc = Qnil;
    p->finalize_proc = Qnil;
    return self;
}

static VALUE rbduckdb_aggregate_function_set_name(VALUE self, VALUE name) {
    rubyDuckDBAggregateFunction *p;
    TypedData_Get_Struct(self, rubyDuckDBAggregateFunction, &aggregate_function_data_type, p);

    const char *str = StringValuePtr(name);
    duckdb_aggregate_function_set_name(p->aggregate_function, str);

    return self;
}

static VALUE rbduckdb_aggregate_function__set_return_type(VALUE self, VALUE logical_type) {
    rubyDuckDBAggregateFunction *p;
    rubyDuckDBLogicalType *lt;

    TypedData_Get_Struct(self, rubyDuckDBAggregateFunction, &aggregate_function_data_type, p);
    lt = get_struct_logical_type(logical_type);

    duckdb_aggregate_function_set_return_type(p->aggregate_function, lt->logical_type);

    return self;
}

static VALUE rbduckdb_aggregate_function_add_parameter(VALUE self, VALUE logical_type) {
    rubyDuckDBAggregateFunction *p;
    rubyDuckDBLogicalType *lt;

    TypedData_Get_Struct(self, rubyDuckDBAggregateFunction, &aggregate_function_data_type, p);
    lt = get_struct_logical_type(logical_type);

    duckdb_aggregate_function_add_parameter(p->aggregate_function, lt->logical_type);

    return self;
}

/*
 * Build a Ruby Hash key from the state's embedded ID.
 * Used for the g_aggregate_state_registry GC root.
 */
static inline VALUE state_registry_key(ruby_aggregate_state *state) {
    return ULL2NUM(state->state_id);
}

/*
 * Store (or update) a Ruby VALUE in the global state registry so that
 * it stays reachable by the GC for the lifetime of the aggregate state.
 */
static inline void state_registry_store(ruby_aggregate_state *state, VALUE value) {
    rb_hash_aset(g_aggregate_state_registry, state_registry_key(state), value);
}

/*
 * Remove a state entry from the registry.  Safe to call even if the
 * entry was already removed (rb_hash_delete is a no-op for missing keys).
 */
static inline void state_registry_remove(ruby_aggregate_state *state) {
    rb_hash_delete(g_aggregate_state_registry, state_registry_key(state));
}

/*
 * Report a pending Ruby exception to DuckDB via
 * duckdb_aggregate_function_set_error and clear it from errinfo.
 * Caller must only invoke this when rb_protect reported exception_state != 0.
 */
static void report_ruby_error_to_duckdb(duckdb_function_info info) {
    VALUE errinfo = rb_errinfo();
    if (errinfo != Qnil) {
        VALUE msg = rb_funcall(errinfo, rb_intern("message"), 0);
        duckdb_aggregate_function_set_error(info, StringValueCStr(msg));
    }
    rb_set_errinfo(Qnil);
}

/* state_size callback: constant buffer per state. */
static idx_t state_size_callback(duckdb_function_info info) {
    (void)info;
    return sizeof(ruby_aggregate_state);
}

/* init callback dispatch argument */
struct init_callback_arg {
    rubyDuckDBAggregateFunction *ctx;
    duckdb_function_info info;
    duckdb_aggregate_state state_p;
};

static VALUE call_init_proc(VALUE varg) {
    struct init_callback_arg *arg = (struct init_callback_arg *)varg;
    return rb_funcall(arg->ctx->init_proc, rb_intern("call"), 0);
}

static void execute_init_callback_protected(void *user_data) {
    struct init_callback_arg *arg = (struct init_callback_arg *)user_data;
    ruby_aggregate_state *state = (ruby_aggregate_state *)arg->state_p;
    int exception_state;
    VALUE result;

    /* Initialise buffer to a safe value before calling Ruby. */
    state->ruby_state = Qnil;
    state->state_id = ++g_next_state_id;

    result = rb_protect(call_init_proc, (VALUE)arg, &exception_state);
    if (exception_state) {
        report_ruby_error_to_duckdb(arg->info);
        return;
    }

    state->ruby_state = result;
    state_registry_store(state, result);
}

static void state_init_callback(duckdb_function_info info, duckdb_aggregate_state state_p) {
    rubyDuckDBAggregateFunction *ctx;
    struct init_callback_arg arg;

    ctx = (rubyDuckDBAggregateFunction *)duckdb_aggregate_function_get_extra_info(info);
    if (ctx == NULL || ctx->init_proc == Qnil) {
        /* Defensive: maybe_set_functions only wires callbacks when init_proc
         * is set, so this branch should be unreachable in practice. Zero the
         * buffer anyway to keep the Ruby state slot well-defined. */
        ruby_aggregate_state *state = (ruby_aggregate_state *)state_p;
        state->ruby_state = Qnil;
        state->state_id = 0;
        return;
    }

    arg.ctx = ctx;
    arg.info = info;
    arg.state_p = state_p;

    rbduckdb_function_executor_dispatch(execute_init_callback_protected, &arg);
}

/* No-op update: used when no update_proc has been supplied. */
static void noop_update_callback(duckdb_function_info info,
                                 duckdb_data_chunk input,
                                 duckdb_aggregate_state *states) {
    (void)info;
    (void)input;
    (void)states;
}

/* update callback dispatch argument */
struct update_callback_arg {
    rubyDuckDBAggregateFunction *ctx;
    duckdb_function_info info;
    duckdb_data_chunk input;
    duckdb_aggregate_state *states;
    duckdb_vector *input_vectors;
    duckdb_logical_type *input_types;
    VALUE *args;
    idx_t row_count;
    idx_t col_count;
};

struct update_one_arg {
    VALUE update_proc;
    int argc;
    VALUE *argv;
};

static VALUE call_update_proc(VALUE varg) {
    struct update_one_arg *arg = (struct update_one_arg *)varg;
    return rb_funcallv(arg->update_proc, rb_intern("call"), arg->argc, arg->argv);
}

/*
 * Body of the update callback: allocate input buffers, walk each row,
 * dispatch to the user's update_proc. Runs inside rb_ensure so that
 * update_cleanup_callback always runs — even if rbduckdb_vector_value_at
 * or the Ruby proc call raises, allocated buffers and logical types are
 * released on the unwind path.
 *
 * Ruby exceptions raised by the user's proc are caught inline via
 * rb_protect and reported to DuckDB as scalar errors; other Ruby
 * exceptions (e.g. from vector_value_at) propagate and are cleaned up
 * by rb_ensure.
 */
static VALUE update_process_rows(VALUE varg) {
    struct update_callback_arg *arg = (struct update_callback_arg *)varg;
    ruby_aggregate_state **states = (ruby_aggregate_state **)arg->states;
    idx_t i, j;

    arg->input_vectors = ALLOC_N(duckdb_vector, arg->col_count);
    arg->input_types = ALLOC_N(duckdb_logical_type, arg->col_count);
    arg->args = ALLOC_N(VALUE, arg->col_count + 1);

    for (j = 0; j < arg->col_count; j++) {
        arg->input_vectors[j] = duckdb_data_chunk_get_vector(arg->input, j);
        arg->input_types[j] = duckdb_vector_get_column_type(arg->input_vectors[j]);
    }

    for (i = 0; i < arg->row_count; i++) {
        ruby_aggregate_state *state = states[i];
        struct update_one_arg one;
        int exception_state;
        VALUE ret;

        arg->args[0] = state->ruby_state;
        for (j = 0; j < arg->col_count; j++) {
            arg->args[j + 1] = rbduckdb_vector_value_at(arg->input_vectors[j], arg->input_types[j], i);
        }

        one.update_proc = arg->ctx->update_proc;
        one.argc = (int)(arg->col_count + 1);
        one.argv = arg->args;

        ret = rb_protect(call_update_proc, (VALUE)&one, &exception_state);
        if (exception_state) {
            report_ruby_error_to_duckdb(arg->info);
            return Qnil;
        }

        state->ruby_state = ret;
        state_registry_store(state, ret);
    }

    return Qnil;
}

static VALUE update_cleanup_callback(VALUE varg) {
    struct update_callback_arg *arg = (struct update_callback_arg *)varg;
    idx_t j;

    if (arg->input_types != NULL) {
        for (j = 0; j < arg->col_count; j++) {
            duckdb_destroy_logical_type(&arg->input_types[j]);
        }
        xfree(arg->input_types);
    }
    if (arg->args != NULL) {
        xfree(arg->args);
    }
    if (arg->input_vectors != NULL) {
        xfree(arg->input_vectors);
    }

    return Qnil;
}

static void execute_update_callback_protected(void *user_data) {
    struct update_callback_arg *arg = (struct update_callback_arg *)user_data;
    rb_ensure(update_process_rows, (VALUE)arg, update_cleanup_callback, (VALUE)arg);
}

static void update_callback(duckdb_function_info info,
                            duckdb_data_chunk input,
                            duckdb_aggregate_state *states) {
    rubyDuckDBAggregateFunction *ctx;
    struct update_callback_arg arg;

    ctx = (rubyDuckDBAggregateFunction *)duckdb_aggregate_function_get_extra_info(info);
    if (ctx == NULL || ctx->update_proc == Qnil) {
        return;
    }

    arg.ctx = ctx;
    arg.info = info;
    arg.input = input;
    arg.states = states;
    arg.input_vectors = NULL;
    arg.input_types = NULL;
    arg.args = NULL;
    arg.row_count = duckdb_data_chunk_get_size(input);
    arg.col_count = duckdb_data_chunk_get_column_count(input);

    rbduckdb_function_executor_dispatch(execute_update_callback_protected, &arg);
}

/* No-op combine: Phase 1.0 does not dispatch combine to Ruby. */
static void noop_combine_callback(duckdb_function_info info,
                                  duckdb_aggregate_state *source,
                                  duckdb_aggregate_state *target,
                                  idx_t count) {
    (void)info;
    (void)source;
    (void)target;
    (void)count;
}

/*
 * Fallback combine used when update_proc is supplied but the user did not
 * register a combine_proc via set_combine.
 *
 * DuckDB invokes combine even for single-partition aggregates: after update
 * has accumulated values into a source state, DuckDB freshly initialises a
 * target state and calls combine to merge source into target before finalize.
 *
 * Without a user-provided combine_proc we cannot perform an arbitrary merge,
 * so this minimal implementation overwrites target->ruby_state with the
 * source value. This is correct for the common single-group/single-thread
 * path; parallel execution requires the user to supply a combine_proc via
 * set_combine, in which case combine_callback is wired instead of this
 * fallback.
 */
static void default_combine_callback(duckdb_function_info info,
                                     duckdb_aggregate_state *source,
                                     duckdb_aggregate_state *target,
                                     idx_t count) {
    ruby_aggregate_state **src = (ruby_aggregate_state **)source;
    ruby_aggregate_state **tgt = (ruby_aggregate_state **)target;
    idx_t i;
    (void)info;

    for (i = 0; i < count; i++) {
        tgt[i]->ruby_state = src[i]->ruby_state;
        /*
         * Do NOT call any Ruby API here.  This callback is invoked by a
         * DuckDB worker thread that does not hold the GVL; any rb_* call
         * from this context is unsafe and causes a SIGSEGV on Windows.
         *
         * The copied VALUE is already GC-protected via the source state's
         * existing registry entry — which shares the same state_id (because
         * DuckDB memcpy'd the buffer).  The destructor callback will clean
         * up that entry when DuckDB frees the source state.
         */
    }
}

/* combine_callback dispatch argument */
struct combine_callback_arg {
    rubyDuckDBAggregateFunction *ctx;
    duckdb_function_info info;
    duckdb_aggregate_state *source;
    duckdb_aggregate_state *target;
    idx_t count;
};

struct combine_one_arg {
    VALUE combine_proc;
    VALUE source_state;
    VALUE target_state;
};

static VALUE call_combine_proc(VALUE varg) {
    struct combine_one_arg *arg = (struct combine_one_arg *)varg;
    VALUE argv[2];
    argv[0] = arg->source_state;
    argv[1] = arg->target_state;
    return rb_funcallv(arg->combine_proc, rb_intern("call"), 2, argv);
}

static void execute_combine_callback_protected(void *user_data) {
    struct combine_callback_arg *arg = (struct combine_callback_arg *)user_data;
    ruby_aggregate_state **src = (ruby_aggregate_state **)arg->source;
    ruby_aggregate_state **tgt = (ruby_aggregate_state **)arg->target;
    idx_t i;

    for (i = 0; i < arg->count; i++) {
        struct combine_one_arg one;
        int exception_state;
        VALUE ret;

        one.combine_proc = arg->ctx->combine_proc;
        one.source_state = src[i]->ruby_state;
        one.target_state = tgt[i]->ruby_state;

        ret = rb_protect(call_combine_proc, (VALUE)&one, &exception_state);
        if (exception_state) {
            report_ruby_error_to_duckdb(arg->info);
            return;
        }

        tgt[i]->ruby_state = ret;
        state_registry_store(tgt[i], ret);

        /* source state is consumed by combine; release its registry entry
         * so the Ruby VALUE can be GC'd. */
        state_registry_remove(src[i]);
    }
}

static void combine_callback(duckdb_function_info info,
                             duckdb_aggregate_state *source,
                             duckdb_aggregate_state *target,
                             idx_t count) {
    rubyDuckDBAggregateFunction *ctx;
    struct combine_callback_arg arg;

    ctx = (rubyDuckDBAggregateFunction *)duckdb_aggregate_function_get_extra_info(info);
    if (ctx == NULL || ctx->combine_proc == Qnil) {
        return;
    }

    arg.ctx = ctx;
    arg.info = info;
    arg.source = source;
    arg.target = target;
    arg.count = count;

    rbduckdb_function_executor_dispatch(execute_combine_callback_protected, &arg);
}

/* finalize callback dispatch argument */
struct finalize_callback_arg {
    rubyDuckDBAggregateFunction *ctx;
    duckdb_function_info info;
    duckdb_aggregate_state *source_p;
    duckdb_vector result;
    idx_t count;
    idx_t offset;
};

struct finalize_one_arg {
    VALUE finalize_proc;
    VALUE ruby_state;
};

static VALUE call_finalize_proc(VALUE varg) {
    struct finalize_one_arg *arg = (struct finalize_one_arg *)varg;
    return rb_funcall(arg->finalize_proc, rb_intern("call"), 1, arg->ruby_state);
}

struct vector_set_arg {
    duckdb_vector vector;
    duckdb_logical_type element_type;
    idx_t index;
    VALUE value;
};

static VALUE call_vector_set_value_at(VALUE varg) {
    struct vector_set_arg *a = (struct vector_set_arg *)varg;
    rbduckdb_vector_set_value_at(a->vector, a->element_type, a->index, a->value);
    return Qnil;
}

static void execute_finalize_callback_protected(void *user_data) {
    struct finalize_callback_arg *arg = (struct finalize_callback_arg *)user_data;
    ruby_aggregate_state **states = (ruby_aggregate_state **)arg->source_p;
    duckdb_logical_type result_type = duckdb_vector_get_column_type(arg->result);
    idx_t i;

    for (i = 0; i < arg->count; i++) {
        ruby_aggregate_state *state = states[i];
        struct finalize_one_arg one;
        struct vector_set_arg vsa;
        int exception_state;
        VALUE ret;

        one.finalize_proc = arg->ctx->finalize_proc;
        one.ruby_state = state->ruby_state;

        ret = rb_protect(call_finalize_proc, (VALUE)&one, &exception_state);
        if (exception_state) {
            report_ruby_error_to_duckdb(arg->info);
            goto cleanup;
        }

        vsa.vector = arg->result;
        vsa.element_type = result_type;
        vsa.index = arg->offset + i;
        vsa.value = ret;

        rb_protect(call_vector_set_value_at, (VALUE)&vsa, &exception_state);
        if (exception_state) {
            report_ruby_error_to_duckdb(arg->info);
            goto cleanup;
        }

        /* Release Ruby state from the GC registry. */
        state_registry_remove(state);
    }

cleanup:
    duckdb_destroy_logical_type(&result_type);
}

static void finalize_callback(duckdb_function_info info,
                              duckdb_aggregate_state *source,
                              duckdb_vector result,
                              idx_t count,
                              idx_t offset) {
    rubyDuckDBAggregateFunction *ctx;
    struct finalize_callback_arg arg;

    ctx = (rubyDuckDBAggregateFunction *)duckdb_aggregate_function_get_extra_info(info);
    if (ctx == NULL || ctx->finalize_proc == Qnil) {
        return;
    }

    arg.ctx = ctx;
    arg.info = info;
    arg.source_p = source;
    arg.result = result;
    arg.count = count;
    arg.offset = offset;

    rbduckdb_function_executor_dispatch(execute_finalize_callback_protected, &arg);
}

/* destroy_callback dispatch argument */
struct destroy_callback_arg {
    duckdb_aggregate_state *states;
    idx_t count;
};

static void execute_destroy_callback(void *data) {
    struct destroy_callback_arg *arg = (struct destroy_callback_arg *)data;
    ruby_aggregate_state **s = (ruby_aggregate_state **)arg->states;
    idx_t i;
    for (i = 0; i < arg->count; i++) {
        state_registry_remove(s[i]);
    }
}

/*
 * Called by DuckDB when it frees aggregate state buffers.  On success paths
 * this runs after finalize has already removed the final-state entries, so
 * the delete is a harmless no-op for those; for intermediate states created
 * by DuckDB's internal memcpy, this is the only cleanup path.
 *
 * Dispatches through the executor thread so that rb_hash_delete is called
 * with the GVL held.
 *
 * The executor thread is guaranteed to be running because
 * maybe_set_functions() calls rbduckdb_function_executor_ensure_started()
 * before registering this destructor.
 */
static void destroy_callback(duckdb_aggregate_state *states, idx_t count) {
    struct destroy_callback_arg arg;
    arg.states = states;
    arg.count = count;
    rbduckdb_function_executor_dispatch(execute_destroy_callback, &arg);
}

/*
 * Wire up all 5 DuckDB aggregate callbacks on the underlying aggregate_function.
 * Called once both init_proc and finalize_proc have been supplied.
 */
static void maybe_set_functions(rubyDuckDBAggregateFunction *p) {
    if (p->init_proc == Qnil || p->finalize_proc == Qnil) {
        return;
    }
    duckdb_aggregate_function_set_extra_info(p->aggregate_function, p, NULL);
    duckdb_aggregate_function_set_functions(
        p->aggregate_function,
        state_size_callback,
        state_init_callback,
        (p->update_proc != Qnil) ? update_callback : noop_update_callback,
        (p->combine_proc != Qnil) ? combine_callback :
            ((p->update_proc != Qnil) ? default_combine_callback : noop_combine_callback),
        finalize_callback);
    duckdb_aggregate_function_set_destructor(p->aggregate_function, destroy_callback);

    /* Ensure the global executor thread is running for multi-thread dispatch.
     * Deferred until callbacks are actually wired to DuckDB. */
    rbduckdb_function_executor_ensure_started();
}

/* :nodoc: */
static VALUE rbduckdb_aggregate_function_set_init(VALUE self) {
    rubyDuckDBAggregateFunction *p;

    if (!rb_block_given_p()) {
        rb_raise(rb_eArgError, "block is required");
    }

    TypedData_Get_Struct(self, rubyDuckDBAggregateFunction, &aggregate_function_data_type, p);
    p->init_proc = rb_block_proc();

    maybe_set_functions(p);

    return self;
}

/* :nodoc: */
static VALUE rbduckdb_aggregate_function_set_update(VALUE self) {
    rubyDuckDBAggregateFunction *p;

    if (!rb_block_given_p()) {
        rb_raise(rb_eArgError, "block is required");
    }

    TypedData_Get_Struct(self, rubyDuckDBAggregateFunction, &aggregate_function_data_type, p);
    p->update_proc = rb_block_proc();

    maybe_set_functions(p);

    return self;
}

/* :nodoc: */
static VALUE rbduckdb_aggregate_function_set_combine(VALUE self) {
    rubyDuckDBAggregateFunction *p;

    if (!rb_block_given_p()) {
        rb_raise(rb_eArgError, "block is required");
    }

    TypedData_Get_Struct(self, rubyDuckDBAggregateFunction, &aggregate_function_data_type, p);
    p->combine_proc = rb_block_proc();

    maybe_set_functions(p);

    return self;
}

/* :nodoc: */
static VALUE rbduckdb_aggregate_function_set_finalize(VALUE self) {
    rubyDuckDBAggregateFunction *p;

    if (!rb_block_given_p()) {
        rb_raise(rb_eArgError, "block is required");
    }

    TypedData_Get_Struct(self, rubyDuckDBAggregateFunction, &aggregate_function_data_type, p);
    p->finalize_proc = rb_block_proc();

    maybe_set_functions(p);

    return self;
}

static VALUE rbduckdb_aggregate_function_set_special_handling(VALUE self) {
    rubyDuckDBAggregateFunction *p;
    TypedData_Get_Struct(self, rubyDuckDBAggregateFunction, &aggregate_function_data_type, p);
    duckdb_aggregate_function_set_special_handling(p->aggregate_function);
    return self;
}

/* Returns the number of Ruby states currently tracked in the registry. */
static VALUE aggregate_function_state_registry_size(VALUE klass) {
    (void)klass;
    return LONG2NUM((long)RHASH_SIZE(g_aggregate_state_registry));
}

void rbduckdb_init_duckdb_aggregate_function(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBAggregateFunction = rb_define_class_under(mDuckDB, "AggregateFunction", rb_cObject);
    rb_define_alloc_func(cDuckDBAggregateFunction, allocate);
    rb_define_method(cDuckDBAggregateFunction, "initialize", duckdb_aggregate_function_initialize, 0);
    rb_define_method(cDuckDBAggregateFunction, "name=", rbduckdb_aggregate_function_set_name, 1);
    rb_define_private_method(cDuckDBAggregateFunction, "_set_return_type", rbduckdb_aggregate_function__set_return_type, 1);
    rb_define_private_method(cDuckDBAggregateFunction, "_add_parameter", rbduckdb_aggregate_function_add_parameter, 1);
    rb_define_method(cDuckDBAggregateFunction, "set_init", rbduckdb_aggregate_function_set_init, 0);
    rb_define_method(cDuckDBAggregateFunction, "set_update", rbduckdb_aggregate_function_set_update, 0);
    rb_define_method(cDuckDBAggregateFunction, "set_combine", rbduckdb_aggregate_function_set_combine, 0);
    rb_define_method(cDuckDBAggregateFunction, "set_finalize", rbduckdb_aggregate_function_set_finalize, 0);
    rb_define_method(cDuckDBAggregateFunction, "set_special_handling", rbduckdb_aggregate_function_set_special_handling, 0);
    rb_define_singleton_method(cDuckDBAggregateFunction, "_state_registry_size",
                               aggregate_function_state_registry_size, 0);

    g_aggregate_state_registry = rb_hash_new();
    rb_gc_register_mark_object(g_aggregate_state_registry);
}
