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
