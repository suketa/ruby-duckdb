#include "ruby-duckdb.h"

/*
 * Cross-platform threading primitives.
 * MSVC (mswin) does not provide <pthread.h>.
 * MinGW-w64 (mingw, ucrt) provides <pthread.h> via winpthreads.
 *
 * See also: FFI gem's approach in ext/ffi_c/Function.c
 *   https://github.com/ffi/ffi/blob/master/ext/ffi_c/Function.c
 */
#ifdef _MSC_VER
#include <windows.h>
#else
#include <pthread.h>
#endif

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
    int done;
#ifdef _MSC_VER
    CRITICAL_SECTION done_lock;
    CONDITION_VARIABLE done_cond;
#else
    pthread_mutex_t done_mutex;
    pthread_cond_t done_cond;
#endif
    struct callback_request *next;
};

/* Global executor state */
#ifdef _MSC_VER
static CRITICAL_SECTION g_executor_lock;
static CONDITION_VARIABLE g_executor_cond;
static int g_sync_initialized = 0;
#else
static pthread_mutex_t g_executor_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_executor_cond = PTHREAD_COND_INITIALIZER;
#endif
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

/* Data passed to the executor wait function */
struct executor_wait_data {
    struct callback_request *request;
    int stop;
};

/* Runs without GVL: blocks on condvar waiting for a callback request */
static void *executor_wait_func(void *data) {
    struct executor_wait_data *w = (struct executor_wait_data *)data;

    w->request = NULL;

#ifdef _MSC_VER
    EnterCriticalSection(&g_executor_lock);
    while (!w->stop && g_request_list == NULL) {
        SleepConditionVariableCS(&g_executor_cond, &g_executor_lock, INFINITE);
    }
    if (g_request_list != NULL) {
        w->request = g_request_list;
        g_request_list = g_request_list->next;
    }
    LeaveCriticalSection(&g_executor_lock);
#else
    pthread_mutex_lock(&g_executor_mutex);
    while (!w->stop && g_request_list == NULL) {
        pthread_cond_wait(&g_executor_cond, &g_executor_mutex);
    }
    if (g_request_list != NULL) {
        w->request = g_request_list;
        g_request_list = g_request_list->next;
    }
    pthread_mutex_unlock(&g_executor_mutex);
#endif

    return NULL;
}

/* Unblock function: called by Ruby to interrupt the executor (e.g., VM shutdown) */
static void executor_stop_func(void *data) {
    struct executor_wait_data *w = (struct executor_wait_data *)data;

#ifdef _MSC_VER
    EnterCriticalSection(&g_executor_lock);
    w->stop = 1;
    WakeConditionVariable(&g_executor_cond);
    LeaveCriticalSection(&g_executor_lock);
#else
    pthread_mutex_lock(&g_executor_mutex);
    w->stop = 1;
    pthread_cond_signal(&g_executor_cond);
    pthread_mutex_unlock(&g_executor_mutex);
#endif
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

            /* Execute the callback with the GVL held */
            req->cb(req->user_data);

            /* Signal the DuckDB worker thread that the callback is done */
#ifdef _MSC_VER
            EnterCriticalSection(&req->done_lock);
            req->done = 1;
            WakeConditionVariable(&req->done_cond);
            LeaveCriticalSection(&req->done_lock);
#else
            pthread_mutex_lock(&req->done_mutex);
            req->done = 1;
            pthread_cond_signal(&req->done_cond);
            pthread_mutex_unlock(&req->done_mutex);
#endif
        }
    }

    return Qnil;
}

void rbduckdb_function_executor_ensure_started(void) {
    if (g_executor_started) return;

#ifdef _MSC_VER
    if (!g_sync_initialized) {
        InitializeCriticalSection(&g_executor_lock);
        InitializeConditionVariable(&g_executor_cond);
        g_sync_initialized = 1;
    }
#endif

    if (g_proxy_threads == Qnil) {
        g_proxy_threads = rb_ary_new();
        rb_global_variable(&g_proxy_threads);
    }

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

#ifdef _MSC_VER
    InitializeCriticalSection(&req.done_lock);
    InitializeConditionVariable(&req.done_cond);

    /* Enqueue the request */
    EnterCriticalSection(&g_executor_lock);
    req.next = g_request_list;
    g_request_list = &req;
    WakeConditionVariable(&g_executor_cond);
    LeaveCriticalSection(&g_executor_lock);

    /* Wait for the executor to process our callback */
    EnterCriticalSection(&req.done_lock);
    while (!req.done) {
        SleepConditionVariableCS(&req.done_cond, &req.done_lock, INFINITE);
    }
    LeaveCriticalSection(&req.done_lock);

    DeleteCriticalSection(&req.done_lock);
#else
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
#endif
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
#ifdef _MSC_VER
    CRITICAL_SECTION lock;
    CONDITION_VARIABLE request_cond;
    CONDITION_VARIABLE request_done_cond;
    CONDITION_VARIABLE thread_exit_cond;
#else
    pthread_mutex_t lock;
    pthread_cond_t request_cond;
    pthread_cond_t request_done_cond;
    pthread_cond_t thread_exit_cond;
#endif
};

/* Runs without GVL: the proxy waits for a callback request */
static void *proxy_wait_func(void *data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;

#ifdef _MSC_VER
    EnterCriticalSection(&proxy->lock);
    while (!proxy->stop_requested && !proxy->has_request) {
        SleepConditionVariableCS(&proxy->request_cond, &proxy->lock, INFINITE);
    }
    LeaveCriticalSection(&proxy->lock);
#else
    pthread_mutex_lock(&proxy->lock);
    while (!proxy->stop_requested && !proxy->has_request) {
        pthread_cond_wait(&proxy->request_cond, &proxy->lock);
    }
    pthread_mutex_unlock(&proxy->lock);
#endif

    return NULL;
}

/* Unblock function for the proxy thread (VM shutdown or Thread#kill) */
static void proxy_stop_func(void *data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;

#ifdef _MSC_VER
    EnterCriticalSection(&proxy->lock);
    proxy->stop_requested = 1;
    WakeConditionVariable(&proxy->request_cond);
    LeaveCriticalSection(&proxy->lock);
#else
    pthread_mutex_lock(&proxy->lock);
    proxy->stop_requested = 1;
    pthread_cond_signal(&proxy->request_cond);
    pthread_mutex_unlock(&proxy->lock);
#endif
}

/* The proxy thread main loop. Runs as the body of rb_ensure (see below). */
static VALUE proxy_loop_body(VALUE data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;

    while (!proxy->stop_requested) {
        /* Release the GVL and wait for a request */
        rb_thread_call_without_gvl(proxy_wait_func, proxy, proxy_stop_func, proxy);

        if (proxy->stop_requested) break;

        if (proxy->has_request) {
            /* Execute the callback with the GVL held */
            proxy->cb(proxy->user_data);

            /* Signal completion to the DuckDB worker thread */
#ifdef _MSC_VER
            EnterCriticalSection(&proxy->lock);
            proxy->has_request = 0;
            proxy->request_done = 1;
            WakeConditionVariable(&proxy->request_done_cond);
            LeaveCriticalSection(&proxy->lock);
#else
            pthread_mutex_lock(&proxy->lock);
            proxy->has_request = 0;
            proxy->request_done = 1;
            pthread_cond_signal(&proxy->request_done_cond);
            pthread_mutex_unlock(&proxy->lock);
#endif
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
#ifdef _MSC_VER
    EnterCriticalSection(&proxy->lock);
    proxy->thread_exited = 1;
    WakeConditionVariable(&proxy->thread_exit_cond);
    LeaveCriticalSection(&proxy->lock);
#else
    pthread_mutex_lock(&proxy->lock);
    proxy->thread_exited = 1;
    pthread_cond_signal(&proxy->thread_exit_cond);
    pthread_mutex_unlock(&proxy->lock);
#endif

    return Qnil;
}

/* The proxy thread entry point (Ruby thread). */
static VALUE proxy_thread_func(void *data) {
    return rb_ensure(proxy_loop_body, (VALUE)data, proxy_cleanup, (VALUE)data);
}

struct worker_proxy *rbduckdb_worker_proxy_create(void) {
    /*
     * Use calloc (not xcalloc): rbduckdb_worker_proxy_destroy frees the struct
     * from a non-Ruby thread where xfree is unsafe.
     */
    struct worker_proxy *proxy = calloc(1, sizeof(struct worker_proxy));
    if (proxy == NULL) {
        rb_raise(rb_eNoMemError, "failed to allocate worker_proxy");
    }

    proxy->stop_requested = 0;
    proxy->has_request = 0;
    proxy->request_done = 0;
    proxy->thread_exited = 0;

#ifdef _MSC_VER
    InitializeCriticalSection(&proxy->lock);
    InitializeConditionVariable(&proxy->request_cond);
    InitializeConditionVariable(&proxy->request_done_cond);
    InitializeConditionVariable(&proxy->thread_exit_cond);
#else
    pthread_mutex_init(&proxy->lock, NULL);
    pthread_cond_init(&proxy->request_cond, NULL);
    pthread_cond_init(&proxy->request_done_cond, NULL);
    pthread_cond_init(&proxy->thread_exit_cond, NULL);
#endif

    /*
     * Lazy-init the GC-protection array so create never silently skips it (see
     * the g_proxy_threads comment above); create runs with the GVL, so safe.
     */
    if (g_proxy_threads == Qnil) {
        g_proxy_threads = rb_ary_new();
        rb_global_variable(&g_proxy_threads);
    }

    proxy->ruby_thread = rb_thread_create(proxy_thread_func, proxy);
    rb_ary_push(g_proxy_threads, proxy->ruby_thread);

    return proxy;
}

/* Blocks until the proxy thread has fully exited. Runs without the GVL. */
static void *proxy_join_func(void *data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;

#ifdef _MSC_VER
    EnterCriticalSection(&proxy->lock);
    while (!proxy->thread_exited) {
        SleepConditionVariableCS(&proxy->thread_exit_cond, &proxy->lock, INFINITE);
    }
    LeaveCriticalSection(&proxy->lock);
#else
    pthread_mutex_lock(&proxy->lock);
    while (!proxy->thread_exited) {
        pthread_cond_wait(&proxy->thread_exit_cond, &proxy->lock);
    }
    pthread_mutex_unlock(&proxy->lock);
#endif

    return NULL;
}

void rbduckdb_worker_proxy_destroy(void *data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;
    if (proxy == NULL) return;

    /* Ask the proxy thread to stop. */
#ifdef _MSC_VER
    EnterCriticalSection(&proxy->lock);
    proxy->stop_requested = 1;
    WakeConditionVariable(&proxy->request_cond);
    LeaveCriticalSection(&proxy->lock);
#else
    pthread_mutex_lock(&proxy->lock);
    proxy->stop_requested = 1;
    pthread_cond_signal(&proxy->request_cond);
    pthread_mutex_unlock(&proxy->lock);
#endif

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

    /* The proxy thread is gone; tear down OS primitives and free the struct. */
#ifdef _MSC_VER
    DeleteCriticalSection(&proxy->lock);
#else
    pthread_cond_destroy(&proxy->thread_exit_cond);
    pthread_cond_destroy(&proxy->request_done_cond);
    pthread_cond_destroy(&proxy->request_cond);
    pthread_mutex_destroy(&proxy->lock);
#endif

    free(proxy);
}

void rbduckdb_function_executor_dispatch(rbduckdb_function_callback_t cb, void *user_data) {
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
    } else {
        /* Case 3: Non-Ruby thread - dispatch to executor */
        dispatch_callback_to_executor(cb, user_data);
    }
}
