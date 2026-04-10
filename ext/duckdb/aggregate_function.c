#include "ruby-duckdb.h"

VALUE cDuckDBAggregateFunction;

/*
 * Global Ruby Hash used to keep aggregate state Ruby VALUEs alive during
 * aggregation. Keys identify the state buffer pointer (see state_registry_key),
 * values are the Ruby VALUE returned from the user's init_proc and later passed
 * to finalize_proc.
 *
 * Protected from GC via rb_gc_register_mark_object on init.
 */
static VALUE g_aggregate_state_registry;

typedef struct {
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
static VALUE rbduckdb_aggregate_function_set_finalize(VALUE self);

static const rb_data_type_t aggregate_function_data_type = {
    "DuckDB/AggregateFunction",
    {mark, deallocate, memsize, compact},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void mark(void *ctx) {
    rubyDuckDBAggregateFunction *p = (rubyDuckDBAggregateFunction *)ctx;
    rb_gc_mark_movable(p->init_proc);
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
 * Build a Ruby Hash key that uniquely identifies a state buffer pointer.
 * Used for the g_aggregate_state_registry GC root.
 */
static inline VALUE state_registry_key(ruby_aggregate_state *state) {
    return ULL2NUM((unsigned long long)(uintptr_t)state);
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

    result = rb_protect(call_init_proc, (VALUE)arg, &exception_state);
    if (exception_state) {
        report_ruby_error_to_duckdb(arg->info);
        return;
    }

    state->ruby_state = result;
    rb_hash_aset(g_aggregate_state_registry, state_registry_key(state), result);
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
        return;
    }

    arg.ctx = ctx;
    arg.info = info;
    arg.state_p = state_p;

    rbduckdb_function_executor_dispatch(execute_init_callback_protected, &arg);
}

/* No-op update: Phase 1.0 does not dispatch update to Ruby. */
static void noop_update_callback(duckdb_function_info info,
                                 duckdb_data_chunk input,
                                 duckdb_aggregate_state *states) {
    (void)info;
    (void)input;
    (void)states;
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

static void execute_finalize_callback_protected(void *user_data) {
    struct finalize_callback_arg *arg = (struct finalize_callback_arg *)user_data;
    ruby_aggregate_state **states = (ruby_aggregate_state **)arg->source_p;
    /* Phase 1.0: result type is hardcoded to BIGINT. Non-BIGINT return types
     * will be supported in a later phase once vector_set_value_at-style
     * type dispatch is shared between scalar and aggregate functions. */
    int64_t *result_data = (int64_t *)duckdb_vector_get_data(arg->result);
    idx_t i;

    for (i = 0; i < arg->count; i++) {
        ruby_aggregate_state *state = states[i];
        struct finalize_one_arg one;
        int exception_state;
        VALUE ret;

        one.finalize_proc = arg->ctx->finalize_proc;
        one.ruby_state = state->ruby_state;

        ret = rb_protect(call_finalize_proc, (VALUE)&one, &exception_state);
        if (exception_state) {
            report_ruby_error_to_duckdb(arg->info);
            return;
        }

        result_data[arg->offset + i] = NUM2LL(ret);

        /* Release Ruby state from the GC registry. */
        rb_hash_delete(g_aggregate_state_registry, state_registry_key(state));
    }
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
        noop_update_callback,
        noop_combine_callback,
        finalize_callback);

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
    rb_define_method(cDuckDBAggregateFunction, "set_finalize", rbduckdb_aggregate_function_set_finalize, 0);

    g_aggregate_state_registry = rb_hash_new();
    rb_gc_register_mark_object(g_aggregate_state_registry);
}
