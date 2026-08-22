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

/*
 * Addresses of every state buffer DuckDB has initialised and not yet
 * destroyed, used as a set: keys are the buffer addresses, the values are
 * unused.
 *
 * This exists only to make a state pointer testable without dereferencing it.
 * DuckDB's CAPIAggregateUpdate flattens the input vectors but not the state
 * vector, although its combine and finalize counterparts both flatten theirs.
 * A window frame that is constant over the whole partition -- `OVER ()` --
 * makes DuckDB pass a constant vector holding one pointer while reporting the
 * partition's full row count, so states[1] onwards read off the end of an
 * eight-byte allocation and yield unrelated heap bytes. Dereferencing those to
 * reach state_id is the crash; membership here is the only test that does not.
 * See issue #1446.
 *
 * Protected from GC via rb_gc_register_mark_object on init.
 */
static VALUE g_aggregate_state_addresses;

typedef struct {
    unsigned long long state_id;
    /* The address this state was initialised at. DuckDB rolls partial states
     * up by memcpy'ing the whole buffer elsewhere and destroying the copy, so
     * the copy is the only thing that can still name the original address --
     * and that address has to leave g_aggregate_state_addresses when the state
     * dies, or the set grows by one entry per query. */
    void *origin;
} ruby_aggregate_state;

static void mark(void *);
static void deallocate(void *);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static void compact(void *);
static VALUE aggregate_function_initialize(VALUE self);
static VALUE aggregate_function_set_name(VALUE self, VALUE name);
static VALUE aggregate_function__set_return_type(VALUE self, VALUE logical_type);
static VALUE aggregate_function__add_parameter(VALUE self, VALUE logical_type);
static VALUE aggregate_function__set_init(VALUE self);
static VALUE aggregate_function__set_update(VALUE self);
static VALUE aggregate_function__set_combine(VALUE self);
static VALUE aggregate_function__set_finalize(VALUE self);
static VALUE aggregate_function__set_special_handling(VALUE self);

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

rubyDuckDBAggregateFunction *rbduckdb_get_struct_aggregate_function(VALUE obj) {
    rubyDuckDBAggregateFunction *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBAggregateFunction, &aggregate_function_data_type, ctx);
    return ctx;
}

static VALUE aggregate_function_initialize(VALUE self) {
    rubyDuckDBAggregateFunction *p;
    TypedData_Get_Struct(self, rubyDuckDBAggregateFunction, &aggregate_function_data_type, p);
    p->aggregate_function = duckdb_create_aggregate_function();
    p->init_proc = Qnil;
    p->update_proc = Qnil;
    p->combine_proc = Qnil;
    p->finalize_proc = Qnil;
    p->special_handling = false;
    return self;
}

static VALUE aggregate_function_set_name(VALUE self, VALUE name) {
    rubyDuckDBAggregateFunction *p;
    TypedData_Get_Struct(self, rubyDuckDBAggregateFunction, &aggregate_function_data_type, p);

    const char *str = StringValuePtr(name);
    duckdb_aggregate_function_set_name(p->aggregate_function, str);

    return self;
}

static VALUE aggregate_function__set_return_type(VALUE self, VALUE logical_type) {
    rubyDuckDBAggregateFunction *p;
    rubyDuckDBLogicalType *lt;

    TypedData_Get_Struct(self, rubyDuckDBAggregateFunction, &aggregate_function_data_type, p);
    lt = rbduckdb_get_struct_logical_type(logical_type);

    duckdb_aggregate_function_set_return_type(p->aggregate_function, lt->logical_type);

    return self;
}

static VALUE aggregate_function__add_parameter(VALUE self, VALUE logical_type) {
    rubyDuckDBAggregateFunction *p;
    rubyDuckDBLogicalType *lt;

    TypedData_Get_Struct(self, rubyDuckDBAggregateFunction, &aggregate_function_data_type, p);
    lt = rbduckdb_get_struct_logical_type(logical_type);

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
 * Read a state's Ruby VALUE back out of the registry.
 *
 * The registry is the only place the VALUE may be read from: a copy cached
 * in the state buffer would be a raw VALUE in a C struct DuckDB allocates,
 * which GC compaction does not update.
 *
 * Returns Qundef, not Qnil, when the state has no entry: nil is a legitimate
 * state (a user's init proc may return it), so the two must not be conflated.
 * Every live state has an entry, so Qundef means a bug in this file; callers
 * report it through report_missing_state_to_duckdb rather than handing the
 * user's proc a state it never returned.
 */
static inline VALUE state_registry_load(ruby_aggregate_state *state) {
    return rb_hash_lookup2(g_aggregate_state_registry, state_registry_key(state), Qundef);
}

/*
 * Remove a state entry from the registry.  Safe to call even if the
 * entry was already removed (rb_hash_delete is a no-op for missing keys).
 */
static inline void state_registry_remove(ruby_aggregate_state *state) {
    rb_hash_delete(g_aggregate_state_registry, state_registry_key(state));
}

/*
 * Membership helpers for g_aggregate_state_addresses. The key is the state
 * buffer's own address, so testing membership never dereferences the pointer;
 * that is the whole point of the set. Only state_address_forget reads through
 * the pointer, and it is called only where doing so is already safe.
 */
static inline VALUE state_address_key(ruby_aggregate_state *state) {
    return ULL2NUM((unsigned long long)(uintptr_t)state);
}

static inline void state_address_add(ruby_aggregate_state *state) {
    rb_hash_aset(g_aggregate_state_addresses, state_address_key(state), Qtrue);
}

/*
 * Only safe on a pointer already known to be dereferenceable -- a live state,
 * or one from a callback whose state vector DuckDB flattened.
 */
static inline void state_address_forget(ruby_aggregate_state *state) {
    rb_hash_delete(g_aggregate_state_addresses, state_address_key(state));
    if (state->origin != (void *)state) {
        rb_hash_delete(g_aggregate_state_addresses,
                       ULL2NUM((unsigned long long)(uintptr_t)state->origin));
    }
}

static inline int state_address_is_live(ruby_aggregate_state *state) {
    if (state == NULL) {
        return 0;
    }
    return RTEST(rb_hash_lookup2(g_aggregate_state_addresses, state_address_key(state), Qfalse));
}

/*
 * Report a pending Ruby exception to DuckDB via
 * duckdb_aggregate_function_set_error and clear it from errinfo.
 * Caller must only invoke this when rb_protect reported exception_state != 0.
 */
static void report_ruby_error_to_duckdb(duckdb_function_info info) {
    VALUE msg = rbduckdb_pending_error_message();
    duckdb_aggregate_function_set_error(info, StringValueCStr(msg));
    RB_GC_GUARD(msg);
}

/*
 * Report a state that has no registry entry.  Not reachable from Ruby code:
 * it means a state was released while DuckDB still held it, which would
 * otherwise pass nil to the user's proc and silently corrupt the result.
 */
static void report_missing_state_to_duckdb(duckdb_function_info info, ruby_aggregate_state *state) {
    char msg[128];

    snprintf(msg, sizeof(msg),
             "aggregate state %llu has no registry entry (ruby-duckdb internal error)",
             (unsigned long long)state->state_id);
    duckdb_aggregate_function_set_error(info, msg);
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

    /* Assign the ID before calling Ruby: the registry entry is keyed on it. */
    state->state_id = ++g_next_state_id;
    state->origin = state;
    state_address_add(state);

    result = rb_protect(call_init_proc, (VALUE)arg, &exception_state);
    if (exception_state) {
        report_ruby_error_to_duckdb(arg->info);
        return;
    }

    state_registry_store(state, result);
}

static void state_init_callback(duckdb_function_info info, duckdb_aggregate_state state_p) {
    rubyDuckDBAggregateFunction *ctx;
    struct init_callback_arg arg;

    ctx = (rubyDuckDBAggregateFunction *)duckdb_aggregate_function_get_extra_info(info);
    if (ctx == NULL || ctx->init_proc == Qnil) {
        /* Defensive: maybe_set_functions only wires callbacks when init_proc
         * is set, so this branch should be unreachable in practice. Zero the
         * ID anyway: IDs start at 1, so this state matches no registry entry
         * and the callbacks report it as a missing state rather than running
         * the user's proc on a state that was never initialised. */
        ruby_aggregate_state *state = (ruby_aggregate_state *)state_p;
        state->state_id = 0;
        state->origin = state;
        state_address_add(state);
        return;
    }

    arg.ctx = ctx;
    arg.info = info;
    arg.state_p = state_p;

    rbduckdb_function_executor_dispatch(execute_init_callback_protected, &arg);
}

/* update callback dispatch argument */
struct update_callback_arg {
    rubyDuckDBAggregateFunction *ctx;
    duckdb_function_info info;
    duckdb_data_chunk input;
    duckdb_aggregate_state *states;
    duckdb_vector *input_vectors;
    duckdb_logical_type *input_types;
    idx_t row_count;
    idx_t col_count;
    /* Non-zero when `states` holds a single pointer shared by every row
     * instead of one per row; set by resolve_state_layout. */
    int constant_states;
};

struct update_one_arg {
    VALUE update_proc;
    VALUE args;
};

static VALUE call_update_proc(VALUE varg) {
    struct update_one_arg *arg = (struct update_one_arg *)varg;
    return rb_apply(arg->update_proc, rb_intern("call"), arg->args);
}

/*
 * DuckDB does not call the destroy callback once update has failed, so the
 * chunk's registry entries have to be dropped here or the Ruby VALUEs leak.
 * Rows in a chunk may share a state; state_registry_remove is idempotent.
 *
 * Entries that are not live states are skipped rather than dropped: under the
 * unflattened constant vector of issue #1446 everything past index 0 is
 * adjacent heap, and reading state_id out of that is the very crash this file
 * is avoiding. Skipping them loses nothing, because a pointer that is not a
 * live state has no registry entry to remove.
 */
static void release_chunk_states(struct update_callback_arg *arg) {
    ruby_aggregate_state **states = (ruby_aggregate_state **)arg->states;
    idx_t i;

    for (i = 0; i < arg->row_count; i++) {
        if (state_address_is_live(states[i])) {
            state_registry_remove(states[i]);
            state_address_forget(states[i]);
        }
    }
}

/*
 * Work out how `states` is laid out before anything dereferences it.
 *
 * DuckDB passes one of two shapes. Normally it is row-indexed: every entry is
 * a state DuckDB has initialised, and rows may share one. When the window
 * frame is constant over the partition, DuckDB instead builds a constant
 * vector holding that partition's single state and forgets to flatten it into
 * row_count copies, so only a short prefix of the array really is that state
 * and the rest is whatever bytes follow an eight-byte allocation.
 *
 * A row-indexed array cannot contain an entry that is not a live state, so one
 * dead entry is proof of the constant layout -- and proof reached without ever
 * dereferencing the pointer, which is what the crash in issue #1446 was.
 *
 * The converse does not hold: the bytes past the constant vector are ordinary
 * heap and sometimes happen to hold live state pointers, so "every entry is
 * live" is only evidence, not proof. It is the safe way round, though: the
 * worst it can do is keep the row-indexed reading DuckDB itself intends, and
 * the entries observed in that prefix are copies of states[0] anyway.
 *
 * What this does rest on is that every state reaching update was initialised
 * at the address it is passed at. That is not true of states in general --
 * DuckDB rolls partial states up by memcpy, and the destroy callback does see
 * copies at addresses it never initialised -- but those copies are made to be
 * combined and finalized, after the rows have been updated, so update itself
 * only ever sees the states DuckDB is still accumulating into.
 */
static int resolve_state_layout(struct update_callback_arg *arg) {
    ruby_aggregate_state **states = (ruby_aggregate_state **)arg->states;
    idx_t i;

    arg->constant_states = 0;

    if (arg->row_count == 0) {
        return 1;
    }

    if (!state_address_is_live(states[0])) {
        /* Not even the one entry DuckDB always fills is a state of ours;
         * there is nothing here that can be aggregated into. */
        duckdb_aggregate_function_set_error(
            arg->info, "aggregate update received a state array holding no live state "
                       "(ruby-duckdb internal error)");
        return 0;
    }

    for (i = 1; i < arg->row_count; i++) {
        if (!state_address_is_live(states[i])) {
            arg->constant_states = 1;
            return 1;
        }
    }

    return 1;
}

/*
 * Body of the update callback: allocate input buffers, walk each row,
 * dispatch to the user's update_proc. Runs inside rb_ensure so that
 * update_cleanup_callback always runs — even if rbduckdb_vector_value_at
 * or the Ruby proc call raises, allocated buffers and logical types are
 * released on the unwind path.
 *
 * Ruby exceptions raised by the user's proc are caught inline via rb_protect
 * and reported to DuckDB; anything else (vector_value_at, or an async
 * exception such as Timeout::Error) unwinds to execute_update_callback_protected.
 *
 * arg->constant_states must already have been set by resolve_state_layout.
 */
static VALUE update_process_rows(VALUE varg) {
    struct update_callback_arg *arg = (struct update_callback_arg *)varg;
    ruby_aggregate_state **states = (ruby_aggregate_state **)arg->states;
    idx_t i, j;

    /* A Ruby Array, not a plain buffer: converting a later column can trigger
     * a GC, and the earlier columns' objects must stay reachable. */
    VALUE args = rb_ary_new_capa((long)arg->col_count + 1);

    arg->input_vectors = ALLOC_N(duckdb_vector, arg->col_count);
    arg->input_types = ALLOC_N(duckdb_logical_type, arg->col_count);

    for (j = 0; j < arg->col_count; j++) {
        arg->input_vectors[j] = duckdb_data_chunk_get_vector(arg->input, j);
        arg->input_types[j] = duckdb_vector_get_column_type(arg->input_vectors[j]);
    }

    for (i = 0; i < arg->row_count; i++) {
        ruby_aggregate_state *state = arg->constant_states ? states[0] : states[i];
        struct update_one_arg one;
        int exception_state;
        VALUE ruby_state;
        VALUE ret;

        /*
         * Without set_special_handling, DuckDB's default behaviour is to
         * skip rows where any input value is NULL.  Check the validity mask
         * of every input column and skip the row if any value is invalid.
         * When special_handling is enabled the callback receives all rows,
         * including those with NULL inputs.
         */
        if (!arg->ctx->special_handling) {
            int has_null = 0;
            for (j = 0; j < arg->col_count; j++) {
                uint64_t *validity = duckdb_vector_get_validity(arg->input_vectors[j]);
                if (validity && !duckdb_validity_row_is_valid(validity, i)) {
                    has_null = 1;
                    break;
                }
            }
            if (has_null) {
                continue;
            }
        }

        ruby_state = state_registry_load(state);
        if (ruby_state == Qundef) {
            report_missing_state_to_duckdb(arg->info, state);
            release_chunk_states(arg);
            RB_GC_GUARD(args);
            return Qnil;
        }

        rb_ary_store(args, 0, ruby_state);
        for (j = 0; j < arg->col_count; j++) {
            rb_ary_store(args, (long)j + 1, rbduckdb_vector_value_at(arg->input_vectors[j], arg->input_types[j], i));
        }

        one.update_proc = arg->ctx->update_proc;
        one.args = args;

        ret = rb_protect(call_update_proc, (VALUE)&one, &exception_state);
        if (exception_state) {
            report_ruby_error_to_duckdb(arg->info);
            release_chunk_states(arg);
            RB_GC_GUARD(args);
            return Qnil;
        }

        state_registry_store(state, ret);
    }

    RB_GC_GUARD(args);

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
    if (arg->input_vectors != NULL) {
        xfree(arg->input_vectors);
    }

    return Qnil;
}

static VALUE update_process_rows_ensured(VALUE varg) {
    return rb_ensure(update_process_rows, varg, update_cleanup_callback, varg);
}

/*
 * The scalar path has always protected its callback body; this one did not, so
 * an exception from anywhere but the user's proc unwound into DuckDB's C++
 * frames. Reachable from ordinary Ruby: Timeout.timeout around a query lands
 * its Timeout::Error in the row loop and wedges the VM.
 */
static void execute_update_callback_protected(void *user_data) {
    struct update_callback_arg *arg = (struct update_callback_arg *)user_data;
    int exception_state;

    if (!resolve_state_layout(arg)) {
        release_chunk_states(arg);
        return;
    }

    rb_protect(update_process_rows_ensured, (VALUE)arg, &exception_state);
    if (exception_state) {
        report_ruby_error_to_duckdb(arg->info);
        release_chunk_states(arg);
    }
}

static void update_callback(duckdb_function_info info,
                            duckdb_data_chunk input,
                            duckdb_aggregate_state *states) {
    rubyDuckDBAggregateFunction *ctx;
    struct update_callback_arg arg;

    ctx = (rubyDuckDBAggregateFunction *)duckdb_aggregate_function_get_extra_info(info);
    if (ctx == NULL) {
        return;
    }
    if (ctx->update_proc == Qnil) {
        /* Reached only if _set_init was called directly (bypassing the Ruby
         * wrapper) without setting an update proc. Raise rather than silently
         * leaving state unchanged. */
        duckdb_aggregate_function_set_error(info, "update callback invoked with no update proc set");
        return;
    }

    arg.ctx = ctx;
    arg.info = info;
    arg.input = input;
    arg.states = states;
    arg.input_vectors = NULL;
    arg.input_types = NULL;
    arg.row_count = duckdb_data_chunk_get_size(input);
    arg.col_count = duckdb_data_chunk_get_column_count(input);
    arg.constant_states = 0;

    rbduckdb_function_executor_dispatch(execute_update_callback_protected, &arg);
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
        one.source_state = state_registry_load(src[i]);
        one.target_state = state_registry_load(tgt[i]);
        if (one.source_state == Qundef || one.target_state == Qundef) {
            report_missing_state_to_duckdb(arg->info, one.source_state == Qundef ? src[i] : tgt[i]);
            return;
        }

        ret = rb_protect(call_combine_proc, (VALUE)&one, &exception_state);
        if (exception_state) {
            report_ruby_error_to_duckdb(arg->info);
            return;
        }

        state_registry_store(tgt[i], ret);

        /* The source entry is left in place: DuckDB reuses one source state
         * across many combine calls (WindowSegmentTree does this for every
         * frame), and the registry is now the only copy of the VALUE, so
         * releasing it here would hand nil to the next combine. The entry is
         * reclaimed by destroy_callback when DuckDB frees the state. */
    }
}

static void combine_callback(duckdb_function_info info,
                             duckdb_aggregate_state *source,
                             duckdb_aggregate_state *target,
                             idx_t count) {
    rubyDuckDBAggregateFunction *ctx;
    struct combine_callback_arg arg;

    ctx = (rubyDuckDBAggregateFunction *)duckdb_aggregate_function_get_extra_info(info);
    if (ctx == NULL) {
        return;
    }
    if (ctx->combine_proc == Qnil) {
        /* Reached only if _set_init was called directly (bypassing the Ruby
         * wrapper) without setting a combine proc. Raise rather than SIGSEGV. */
        duckdb_aggregate_function_set_error(info, "combine callback invoked with no combine proc set");
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
        one.ruby_state = state_registry_load(state);
        if (one.ruby_state == Qundef) {
            report_missing_state_to_duckdb(arg->info, state);
            goto cleanup;
        }

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
        state_address_forget(state);
    }

cleanup:
    /* Clean up registry entries for the current (failed) state and any
       remaining unprocessed states so we don't leak GC-registered objects. */
    for (; i < arg->count; i++) {
        state_registry_remove(states[i]);
        state_address_forget(states[i]);
    }
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
    if (ctx == NULL) {
        return;
    }
    if (ctx->finalize_proc == Qnil) {
        /* Reached only if _set_init was called directly (bypassing the Ruby
         * wrapper) without setting a finalize proc. Raise rather than SIGSEGV. */
        duckdb_aggregate_function_set_error(info, "finalize callback invoked with no finalize proc set");
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
        state_address_forget(s[i]);
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
 * Called once init_proc has been supplied.  combine_proc and finalize_proc are
 * guaranteed non-nil by the Ruby wrapper (set_init injects defaults for all
 * three before calling _set_init).
 */
static void maybe_set_functions(rubyDuckDBAggregateFunction *p) {
    if (p->init_proc == Qnil) {
        return;
    }
    duckdb_aggregate_function_set_extra_info(p->aggregate_function, p, NULL);
    duckdb_aggregate_function_set_functions(
        p->aggregate_function,
        state_size_callback,
        state_init_callback,
        update_callback,
        combine_callback,
        finalize_callback);
    duckdb_aggregate_function_set_destructor(p->aggregate_function, destroy_callback);

    /* Ensure the global executor thread is running for multi-thread dispatch.
     * Deferred until callbacks are actually wired to DuckDB. */
    rbduckdb_function_executor_ensure_started();
}

/* :nodoc: */
static VALUE aggregate_function__set_init(VALUE self) {
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
static VALUE aggregate_function__set_update(VALUE self) {
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
static VALUE aggregate_function__set_combine(VALUE self) {
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
static VALUE aggregate_function__set_finalize(VALUE self) {
    rubyDuckDBAggregateFunction *p;

    if (!rb_block_given_p()) {
        rb_raise(rb_eArgError, "block is required");
    }

    TypedData_Get_Struct(self, rubyDuckDBAggregateFunction, &aggregate_function_data_type, p);
    p->finalize_proc = rb_block_proc();

    maybe_set_functions(p);

    return self;
}

/* :nodoc: */
static VALUE aggregate_function__set_special_handling(VALUE self) {
    rubyDuckDBAggregateFunction *p;
    TypedData_Get_Struct(self, rubyDuckDBAggregateFunction, &aggregate_function_data_type, p);
    p->special_handling = true;
    duckdb_aggregate_function_set_special_handling(p->aggregate_function);
    return self;
}

/* Returns the number of Ruby states currently tracked in the registry. */
static VALUE aggregate_function_s__state_registry_size(VALUE klass) {
    (void)klass;
    return LONG2NUM((long)RHASH_SIZE(g_aggregate_state_registry));
}

void rbduckdb_init_aggregate_function(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBAggregateFunction = rb_define_class_under(mDuckDB, "AggregateFunction", rb_cObject);
    rb_define_alloc_func(cDuckDBAggregateFunction, allocate);
    rb_define_method(cDuckDBAggregateFunction, "initialize", aggregate_function_initialize, 0);
    rb_define_method(cDuckDBAggregateFunction, "set_name", aggregate_function_set_name, 1);
    rb_define_private_method(cDuckDBAggregateFunction, "_set_return_type", aggregate_function__set_return_type, 1);
    rb_define_private_method(cDuckDBAggregateFunction, "_add_parameter", aggregate_function__add_parameter, 1);
    rb_define_private_method(cDuckDBAggregateFunction, "_set_init", aggregate_function__set_init, 0);
    rb_define_private_method(cDuckDBAggregateFunction, "_set_update", aggregate_function__set_update, 0);
    rb_define_private_method(cDuckDBAggregateFunction, "_set_combine", aggregate_function__set_combine, 0);
    rb_define_private_method(cDuckDBAggregateFunction, "_set_finalize", aggregate_function__set_finalize, 0);
    rb_define_private_method(cDuckDBAggregateFunction, "_set_special_handling", aggregate_function__set_special_handling, 0);
    rb_define_singleton_method(cDuckDBAggregateFunction, "_state_registry_size",
                               aggregate_function_s__state_registry_size, 0);

    g_aggregate_state_registry = rb_hash_new();
    rb_gc_register_mark_object(g_aggregate_state_registry);
    g_aggregate_state_addresses = rb_hash_new();
    rb_gc_register_mark_object(g_aggregate_state_addresses);
}
