#include "ruby-duckdb.h"

/*
 * Cross-platform threading primitives.
 * MSVC (mswin) does not provide <pthread.h>.
 * MinGW-w64 (mingw, ucrt) provides <pthread.h> via winpthreads.
 *
 * See also: FFI gem's approach in ext/ffi_c/Function.c
 *   https://github.com/ffi/ffi/blob/master/ext/ffi_c/Function.c
 *
 * This portable sync layer is the only place in this file that splits on
 * platform. Everything below is written once against these wrappers.
 */
#ifdef _MSC_VER
#include <windows.h>

typedef CRITICAL_SECTION sync_mutex_t;
typedef CONDITION_VARIABLE sync_cond_t;

static void sync_mutex_init(sync_mutex_t *m) { InitializeCriticalSection(m); }
static void sync_mutex_destroy(sync_mutex_t *m) { DeleteCriticalSection(m); }
static void sync_lock(sync_mutex_t *m) { EnterCriticalSection(m); }
static void sync_unlock(sync_mutex_t *m) { LeaveCriticalSection(m); }
static void sync_cond_init(sync_cond_t *c) { InitializeConditionVariable(c); }
/* A CONDITION_VARIABLE holds no resources and has no destroy call. */
static void sync_cond_destroy(sync_cond_t *c) { (void)c; }
static void sync_signal(sync_cond_t *c) { WakeConditionVariable(c); }
static void sync_wait(sync_cond_t *c, sync_mutex_t *m) { SleepConditionVariableCS(c, m, INFINITE); }
#else
#include <pthread.h>

typedef pthread_mutex_t sync_mutex_t;
typedef pthread_cond_t sync_cond_t;

static void sync_mutex_init(sync_mutex_t *m) { pthread_mutex_init(m, NULL); }
static void sync_mutex_destroy(sync_mutex_t *m) { pthread_mutex_destroy(m); }
static void sync_lock(sync_mutex_t *m) { pthread_mutex_lock(m); }
static void sync_unlock(sync_mutex_t *m) { pthread_mutex_unlock(m); }
static void sync_cond_init(sync_cond_t *c) { pthread_cond_init(c, NULL); }
static void sync_cond_destroy(sync_cond_t *c) { pthread_cond_destroy(c); }
static void sync_signal(sync_cond_t *c) { pthread_cond_signal(c); }
static void sync_wait(sync_cond_t *c, sync_mutex_t *m) { pthread_cond_wait(c, m); }
#endif

/* Under m: set *flag and wake one waiter on c. */
static void sync_set_and_signal(sync_mutex_t *m, sync_cond_t *c, volatile int *flag) {
    sync_lock(m);
    *flag = 1;
    sync_signal(c);
    sync_unlock(m);
}

/* Under m: block on c until *flag is set. */
static void sync_wait_for_flag(sync_mutex_t *m, sync_cond_t *c, volatile int *flag) {
    sync_lock(m);
    while (!*flag) {
        sync_wait(c, m);
    }
    sync_unlock(m);
}

/*
 * Thread detection functions (available since Ruby 2.3).
 * Used to determine the correct dispatch path for callbacks.
 */
extern int ruby_thread_has_gvl_p(void);
extern int ruby_native_thread_p(void);

/*
 * ============================================================================
 * Global Executor Thread
 * ============================================================================
 *
 * DuckDB calls UDF callbacks from its own worker threads, which are NOT
 * Ruby threads. Ruby's GVL (Global VM Lock) cannot be acquired from
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
    rbduckdb_function_callback_t cb;
    void *user_data;
    volatile int done;
    sync_mutex_t done_lock;
    sync_cond_t done_cond;
    struct callback_request *next;
};

/*
 * Global executor state. The lock and condvar are initialized once by
 * rbduckdb_function_executor_ensure_started, before any worker can use them.
 */
static sync_mutex_t g_executor_lock;
static sync_cond_t g_executor_cond;
static int g_sync_initialized = 0;
static struct callback_request *g_request_list = NULL;
static VALUE g_executor_thread = Qnil;
static int g_executor_started = 0;

/*
 * GC-protection array holding every live per-worker proxy Ruby thread.
 * Proxies are created from non-Ruby init hooks (via the global executor) and
 * are not reachable from any marked object, so without this array the GC could
 * collect a proxy thread while DuckDB still dispatches callbacks to it.
 */
static VALUE g_proxy_threads = Qnil;

/* Lazy-init g_proxy_threads. Must be called with the GVL. */
static void proxy_threads_ensure(void) {
    if (g_proxy_threads == Qnil) {
        g_proxy_threads = rb_ary_new();
        rb_global_variable(&g_proxy_threads);
    }
}

/*
 * Replace embedded NUL bytes so the message can be handed to a C API.
 * Returns str unchanged when there is nothing to replace.
 */
static VALUE message_without_nul(VALUE str) {
    long len = RSTRING_LEN(str);
    long i;
    VALUE copy;
    char *p;

    if (memchr(RSTRING_PTR(str), '\0', (size_t)len) == NULL) {
        return str;
    }

    copy = rb_str_new(RSTRING_PTR(str), len);
    p = RSTRING_PTR(copy);
    for (i = 0; i < len; i++) {
        if (p[i] == '\0') {
            p[i] = ' ';
        }
    }
    return copy;
}

static VALUE pending_error_message(VALUE errinfo) {
    return message_without_nul(rb_obj_as_string(rb_funcall(errinfo, rb_intern("message"), 0)));
}

VALUE rbduckdb_pending_error_message(void) {
    VALUE errinfo = rb_errinfo();
    VALUE msg;
    int state = 0;

    rb_set_errinfo(Qnil);
    if (errinfo == Qnil) {
        return rb_str_new_cstr("unknown error");
    }

    msg = rb_protect(pending_error_message, errinfo, &state);
    if (state) {
        rb_set_errinfo(Qnil);
        return rb_str_new_cstr("an exception was raised, but its message could not be read");
    }
    return msg;
}

/* Data passed to the executor wait function */
struct executor_wait_data {
    struct callback_request *request;
    volatile int stop;
};

/* Runs without GVL: blocks on condvar waiting for a callback request */
static void *executor_wait_func(void *data) {
    struct executor_wait_data *w = (struct executor_wait_data *)data;

    w->request = NULL;

    sync_lock(&g_executor_lock);
    while (!w->stop && g_request_list == NULL) {
        sync_wait(&g_executor_cond, &g_executor_lock);
    }
    if (g_request_list != NULL) {
        w->request = g_request_list;
        g_request_list = g_request_list->next;
    }
    sync_unlock(&g_executor_lock);

    return NULL;
}

/* Unblock function: called by Ruby to interrupt the executor (e.g., VM shutdown) */
static void executor_stop_func(void *data) {
    struct executor_wait_data *w = (struct executor_wait_data *)data;

    sync_set_and_signal(&g_executor_lock, &g_executor_cond, &w->stop);
}

/*
 * Discard an exception a dispatched callback let escape, so that one bad
 * callback cannot take a dispatcher thread down with it and strand every
 * later callback. Interrupt, SystemExit and thread kill still get through.
 */
static void discard_callback_exception(int state) {
    VALUE err = rb_errinfo();

    if (err != Qnil && !rb_obj_is_kind_of(err, rb_eStandardError)) {
        rb_jump_tag(state);
    }
    rb_set_errinfo(Qnil);
}

static VALUE request_run_body(VALUE varg) {
    struct callback_request *req = (struct callback_request *)varg;
    req->cb(req->user_data);
    return Qnil;
}

/* Runs from rb_ensure: the worker waits on done_cond even if the callback raised. */
static VALUE request_signal_done(VALUE varg) {
    struct callback_request *req = (struct callback_request *)varg;

    sync_set_and_signal(&req->done_lock, &req->done_cond, &req->done);
    return Qnil;
}

static VALUE request_run(VALUE varg) {
    return rb_ensure(request_run_body, varg, request_signal_done, varg);
}

/* The executor thread main loop (Ruby thread) */
static VALUE executor_thread_func(void *data) {
    struct executor_wait_data w;
    w.stop = 0;

    while (!w.stop) {
        /* Release GVL and wait for a callback request */
        rb_thread_call_without_gvl(executor_wait_func, &w, executor_stop_func, &w);

        if (w.request != NULL) {
            int state = 0;

            rb_protect(request_run, (VALUE)w.request, &state);
            if (state) {
                discard_callback_exception(state);
            }
        }
    }

    return Qnil;
}

void rbduckdb_function_executor_ensure_started(void) {
    if (g_executor_started) return;

    if (!g_sync_initialized) {
        sync_mutex_init(&g_executor_lock);
        sync_cond_init(&g_executor_cond);
        g_sync_initialized = 1;
    }

    proxy_threads_ensure();

    g_executor_thread = rb_thread_create(executor_thread_func, NULL);
    rb_global_variable(&g_executor_thread);
    g_executor_started = 1;
}

/*
 * Dispatch a callback to the global executor thread.
 * Called from a DuckDB worker thread (non-Ruby thread).
 * The caller blocks until the callback is processed.
 */
static void dispatch_callback_to_executor(rbduckdb_function_callback_t cb, void *user_data) {
    struct callback_request req;

    req.cb = cb;
    req.user_data = user_data;
    req.done = 0;
    req.next = NULL;

    sync_mutex_init(&req.done_lock);
    sync_cond_init(&req.done_cond);

    /* Enqueue the request */
    sync_lock(&g_executor_lock);
    req.next = g_request_list;
    g_request_list = &req;
    sync_signal(&g_executor_cond);
    sync_unlock(&g_executor_lock);

    /* Wait for the executor to process our callback */
    sync_wait_for_flag(&req.done_lock, &req.done_cond, &req.done);

    sync_cond_destroy(&req.done_cond);
    sync_mutex_destroy(&req.done_lock);
}

/* Payload for rb_thread_call_with_gvl wrapper */
struct with_gvl_arg {
    rbduckdb_function_callback_t cb;
    void *user_data;
};

/*
 * Wrapper for rb_thread_call_with_gvl: executes the callback after
 * re-acquiring the GVL. Used when a Ruby thread (without GVL) is the caller.
 */
static void *callback_with_gvl(void *data) {
    struct with_gvl_arg *arg = (struct with_gvl_arg *)data;
    arg->cb(arg->user_data);
    return NULL;
}

/*
 * ============================================================================
 * Per-worker proxy thread
 * ============================================================================
 *
 * One dedicated Ruby thread per DuckDB worker thread. Same hand-off protocol as
 * the global executor (mutex + condvars), but private to a single worker so
 * that callbacks from different workers no longer serialize through one queue.
 *
 * Pattern follows the FFI gem's async callback dispatcher:
 *   https://github.com/ffi/ffi/blob/master/ext/ffi_c/Function.c
 */
struct worker_proxy {
    VALUE ruby_thread;
    volatile int stop_requested;
    rbduckdb_function_callback_t cb;
    void *user_data;
    volatile int has_request;
    volatile int request_done;
    volatile int thread_exited;
    sync_mutex_t lock;
    sync_cond_t request_cond;
    sync_cond_t request_done_cond;
    sync_cond_t thread_exit_cond;
};

/* Runs without GVL: the proxy waits for a callback request */
static void *proxy_wait_func(void *data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;

    sync_lock(&proxy->lock);
    while (!proxy->stop_requested && !proxy->has_request) {
        sync_wait(&proxy->request_cond, &proxy->lock);
    }
    sync_unlock(&proxy->lock);

    return NULL;
}

/* Unblock function for the proxy thread (VM shutdown or Thread#kill) */
static void proxy_stop_func(void *data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;

    sync_set_and_signal(&proxy->lock, &proxy->request_cond, &proxy->stop_requested);
}

static VALUE proxy_run_body(VALUE data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;
    proxy->cb(proxy->user_data);
    return Qnil;
}

/* Runs from rb_ensure: see request_signal_done. */
static VALUE proxy_signal_done(VALUE data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;

    sync_lock(&proxy->lock);
    proxy->has_request = 0;
    proxy->request_done = 1;
    sync_signal(&proxy->request_done_cond);
    sync_unlock(&proxy->lock);

    return Qnil;
}

static VALUE proxy_run(VALUE data) {
    return rb_ensure(proxy_run_body, data, proxy_signal_done, data);
}

/* The proxy thread main loop. Runs as the body of rb_ensure (see below). */
static VALUE proxy_loop_body(VALUE data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;

    while (!proxy->stop_requested) {
        /* Release the GVL and wait for a request */
        rb_thread_call_without_gvl(proxy_wait_func, proxy, proxy_stop_func, proxy);

        if (proxy->stop_requested) break;

        if (proxy->has_request) {
            int state = 0;

            rb_protect(proxy_run, data, &state);
            if (state) {
                discard_callback_exception(state);
            }
        }
    }

    return Qnil;
}

/*
 * Teardown for the proxy thread. Run via rb_ensure so it executes even if an
 * async exception (Thread#kill, VM shutdown via rb_thread_terminate_all)
 * unwinds proxy_loop_body. If it were skipped, thread_exited would stay 0
 * forever and rbduckdb_worker_proxy_destroy's join would deadlock.
 */
static VALUE proxy_cleanup(VALUE data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;

    /* Stop being GC-protected now that we are about to exit */
    if (g_proxy_threads != Qnil) {
        rb_ary_delete(g_proxy_threads, proxy->ruby_thread);
    }

    /*
     * Signal that this thread has finished and no longer touches the proxy
     * struct. Only after this may rbduckdb_worker_proxy_destroy free it.
     */
    sync_set_and_signal(&proxy->lock, &proxy->thread_exit_cond, &proxy->thread_exited);
    return Qnil;
}

/* The proxy thread entry point (Ruby thread). */
static VALUE proxy_thread_func(void *data) {
    return rb_ensure(proxy_loop_body, (VALUE)data, proxy_cleanup, (VALUE)data);
}

/* Tear down the OS primitives and free the struct. The proxy thread must be gone. */
static void proxy_free(struct worker_proxy *proxy) {
    sync_cond_destroy(&proxy->thread_exit_cond);
    sync_cond_destroy(&proxy->request_done_cond);
    sync_cond_destroy(&proxy->request_cond);
    sync_mutex_destroy(&proxy->lock);

    free(proxy);
}

static VALUE proxy_start_thread(VALUE data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;

    proxy->ruby_thread = rb_thread_create(proxy_thread_func, proxy);
    rb_ary_push(g_proxy_threads, proxy->ruby_thread);
    return Qnil;
}

struct worker_proxy *rbduckdb_worker_proxy_create(void) {
    struct worker_proxy *proxy;
    int state = 0;

    /*
     * Before allocating so a raise here leaks nothing; create runs with the
     * GVL, so safe.
     */
    proxy_threads_ensure();

    /*
     * Use calloc (not xcalloc): rbduckdb_worker_proxy_destroy frees the struct
     * from a non-Ruby thread where xfree is unsafe.
     */
    proxy = calloc(1, sizeof(struct worker_proxy));
    if (proxy == NULL) {
        rb_raise(rb_eNoMemError, "failed to allocate worker_proxy");
    }

    proxy->stop_requested = 0;
    proxy->has_request = 0;
    proxy->request_done = 0;
    proxy->thread_exited = 0;

    sync_mutex_init(&proxy->lock);
    sync_cond_init(&proxy->request_cond);
    sync_cond_init(&proxy->request_done_cond);
    sync_cond_init(&proxy->thread_exit_cond);

    rb_protect(proxy_start_thread, (VALUE)proxy, &state);
    if (state) {
        if (proxy->ruby_thread) {
            /*
             * The thread started but could not be registered (rb_ary_push
             * raised NoMemError): stop and join it. Like any destroy, the join
             * has no unblock function; see #1512 for the hang analysis.
             */
            rbduckdb_worker_proxy_destroy(proxy);
        } else {
            /* rb_thread_create raised: no thread ever touched the proxy. */
            proxy_free(proxy);
        }
        rb_jump_tag(state);
    }

    return proxy;
}

struct create_proxy_callback_arg {
    struct worker_proxy *proxy;
};

static VALUE create_proxy_callback(VALUE varg) {
    struct create_proxy_callback_arg *arg = (struct create_proxy_callback_arg *)varg;
    arg->proxy = rbduckdb_worker_proxy_create();
    return Qnil;
}

/*
 * rbduckdb_worker_proxy_create may raise (NoMemError, Thread.new failure),
 * and the executor runs callbacks unprotected - a raise would longjmp past
 * its done-signaling and block the waiting DuckDB worker forever. Swallow
 * the exception instead: the proxy stays NULL and the caller falls back to
 * the global executor.
 */
static void create_proxy_callback_protected(void *user_data) {
    int exception_state;

    rb_protect(create_proxy_callback, (VALUE)user_data, &exception_state);
    if (exception_state) {
        rb_set_errinfo(Qnil);
    }
}

struct worker_proxy *rbduckdb_worker_proxy_create_for_current_thread(void) {
    struct create_proxy_callback_arg arg;

    /* A Ruby calling thread runs the callback inline (Case 1/2); no proxy needed. */
    if (ruby_native_thread_p()) return NULL;

    arg.proxy = NULL;
    rbduckdb_function_executor_dispatch(create_proxy_callback_protected, &arg);
    return arg.proxy;
}

/*
 * Hand a callback to a proxy and block until it completes.
 * Called from the DuckDB worker thread (non-Ruby thread) that owns this proxy.
 */
static void dispatch_callback_to_proxy(struct worker_proxy *proxy, rbduckdb_function_callback_t cb, void *user_data) {
    sync_lock(&proxy->lock);
    proxy->cb = cb;
    proxy->user_data = user_data;
    proxy->request_done = 0;
    proxy->has_request = 1;
    sync_signal(&proxy->request_cond);
    sync_unlock(&proxy->lock);

    sync_wait_for_flag(&proxy->lock, &proxy->request_done_cond, &proxy->request_done);
}

/* Blocks until the proxy thread has fully exited. Runs without the GVL. */
static void *proxy_join_func(void *data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;

    sync_wait_for_flag(&proxy->lock, &proxy->thread_exit_cond, &proxy->thread_exited);
    return NULL;
}

void rbduckdb_worker_proxy_destroy(void *data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;
    if (proxy == NULL) return;

    /* Ask the proxy thread to stop. */
    proxy_stop_func(proxy);

    /*
     * Wait until the proxy thread has fully exited. Before exiting it runs Ruby
     * code (removing itself from the GC-protection array), which needs the GVL.
     * DuckDB may invoke this destructor either from a worker thread (no GVL) or
     * — depending on when it tears down the local state — from a Ruby thread
     * that holds the GVL. In the latter case we must release the GVL while
     * waiting, or the proxy thread could never acquire it and we would deadlock.
     */
    if (ruby_native_thread_p() && ruby_thread_has_gvl_p()) {
        rb_thread_call_without_gvl(proxy_join_func, proxy, NULL, NULL);
    } else {
        proxy_join_func(proxy);
    }

    proxy_free(proxy);
}

void rbduckdb_function_executor_dispatch_via_proxy(rbduckdb_function_callback_t cb, void *user_data, struct worker_proxy *proxy) {
    if (ruby_native_thread_p()) {
        if (ruby_thread_has_gvl_p()) {
            /* Case 1: Ruby thread with GVL - call directly */
            cb(user_data);
        } else {
            /* Case 2: Ruby thread without GVL - reacquire GVL */
            struct with_gvl_arg arg;
            arg.cb = cb;
            arg.user_data = user_data;
            rb_thread_call_with_gvl(callback_with_gvl, &arg);
        }
    } else if (proxy != NULL) {
        /* Case 3a: Non-Ruby thread with a per-worker proxy */
        dispatch_callback_to_proxy(proxy, cb, user_data);
    } else {
        /* Case 3b: Non-Ruby thread - dispatch to the global executor */
        dispatch_callback_to_executor(cb, user_data);
    }
}

void rbduckdb_function_executor_dispatch(rbduckdb_function_callback_t cb, void *user_data) {
    rbduckdb_function_executor_dispatch_via_proxy(cb, user_data, NULL);
}

static VALUE g_function_data_registry = Qnil;
static unsigned long long g_function_data_next_id = 0;

static void function_data_registry_ensure(void) {
    if (g_function_data_registry == Qnil) {
        g_function_data_registry = rb_hash_new();
        rb_global_variable(&g_function_data_registry);
    }
}

void *rbduckdb_function_data_register(VALUE value) {
    unsigned long long id;

    function_data_registry_ensure();
    id = ++g_function_data_next_id;
    rb_hash_aset(g_function_data_registry, ULL2NUM(id), value);
    return (void *)(uintptr_t)id;
}

VALUE rbduckdb_function_data_lookup(void *data) {
    if (data == NULL || g_function_data_registry == Qnil) {
        return Qnil;
    }
    return rb_hash_aref(g_function_data_registry, ULL2NUM((unsigned long long)(uintptr_t)data));
}

void rbduckdb_function_data_release(void *data) {
    if (g_function_data_registry == Qnil) return;
    rb_hash_delete(g_function_data_registry, ULL2NUM((unsigned long long)(uintptr_t)data));
}

void rbduckdb_function_data_destroy(void *data) {
    if (data == NULL) return;
    rbduckdb_function_executor_dispatch(rbduckdb_function_data_release, data);
}
