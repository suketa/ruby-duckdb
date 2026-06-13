#include "ruby-duckdb.h"
#include <errno.h>

static VALUE cDuckDBArrowArrayStream;

typedef struct {
    struct ArrowArrayStream stream;
} rubyDuckDBArrowArrayStream;

/*
 * Heap-allocated context referenced by stream.private_data. Consumers may
 * move the stream struct contents out and keep using the callbacks after
 * the Ruby DuckDB::ArrowArrayStream object is gone, so this context is
 * freed only by the stream release callback, and it holds a reference on
 * the result struct (rbduckdb_result_ref) until then. The release callback
 * must not call any Ruby API: it can run during GC sweep (via deallocate
 * of an unconsumed stream) or from a non-Ruby thread.
 */
typedef struct {
    rubyDuckDBResult *presult_ctx;
    duckdb_arrow_options arrow_options;
    char *last_error;
} arrowArrayStreamContext;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE arrow_array_stream_to_i(VALUE self);
static VALUE arrow_array_stream_arrow_c_stream(VALUE self);
static void stream_set_error(arrowArrayStreamContext *ctx, const char *msg);
static int stream_check_error(arrowArrayStreamContext *ctx, duckdb_error_data error_data);
static int stream_get_schema(struct ArrowArrayStream *stream, struct ArrowSchema *out);
static int stream_get_next(struct ArrowArrayStream *stream, struct ArrowArray *out);
static const char *stream_get_last_error(struct ArrowArrayStream *stream);
static void stream_release(struct ArrowArrayStream *stream);

static const rb_data_type_t arrow_array_stream_data_type = {
    "DuckDB/ArrowArrayStream",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBArrowArrayStream *p = (rubyDuckDBArrowArrayStream *)ctx;

    if (p->stream.release != NULL) {
        p->stream.release(&(p->stream));
    }
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBArrowArrayStream *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBArrowArrayStream));
    return TypedData_Wrap_Struct(klass, &arrow_array_stream_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBArrowArrayStream);
}

/* Context memory is managed with plain malloc/free because the release
 * callback may run outside Ruby's memory bookkeeping. */
static void stream_set_error(arrowArrayStreamContext *ctx, const char *msg) {
    size_t len = strlen(msg) + 1;

    free(ctx->last_error);
    ctx->last_error = malloc(len);
    if (ctx->last_error != NULL) {
        memcpy(ctx->last_error, msg, len);
    }
}

static int stream_check_error(arrowArrayStreamContext *ctx, duckdb_error_data error_data) {
    if (error_data == NULL) {
        return 0;
    }
    if (!duckdb_error_data_has_error(error_data)) {
        duckdb_destroy_error_data(&error_data);
        return 0;
    }
    stream_set_error(ctx, duckdb_error_data_message(error_data));
    duckdb_destroy_error_data(&error_data);
    return EIO;
}

static int stream_get_schema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
    arrowArrayStreamContext *ctx = (arrowArrayStreamContext *)stream->private_data;
    duckdb_error_data error_data;
    duckdb_logical_type *types;
    const char **names;
    idx_t column_count;
    idx_t i;

    column_count = duckdb_column_count(&(ctx->presult_ctx->result));
    types = calloc((size_t)column_count, sizeof(duckdb_logical_type));
    names = calloc((size_t)column_count, sizeof(const char *));
    if (column_count > 0 && (types == NULL || names == NULL)) {
        free(types);
        free(names);
        stream_set_error(ctx, "failed to allocate memory for Arrow schema conversion");
        return ENOMEM;
    }
    for (i = 0; i < column_count; i++) {
        types[i] = duckdb_column_logical_type(&(ctx->presult_ctx->result), i);
        names[i] = duckdb_column_name(&(ctx->presult_ctx->result), i);
    }

    error_data = duckdb_to_arrow_schema(ctx->arrow_options, types, names, column_count, out);

    for (i = 0; i < column_count; i++) {
        duckdb_destroy_logical_type(&types[i]);
    }
    free(types);
    free(names);
    return stream_check_error(ctx, error_data);
}

static int stream_get_next(struct ArrowArrayStream *stream, struct ArrowArray *out) {
    arrowArrayStreamContext *ctx = (arrowArrayStreamContext *)stream->private_data;
    duckdb_data_chunk chunk;
    duckdb_error_data error_data;

    chunk = duckdb_fetch_chunk(ctx->presult_ctx->result);
    if (chunk == NULL) {
        /* End of stream: a released (release == NULL) array. */
        memset(out, 0, sizeof(struct ArrowArray));
        return 0;
    }
    /* duckdb_data_chunk_to_arrow copies the chunk into Arrow-owned buffers,
     * so the chunk can be destroyed right after conversion. */
    error_data = duckdb_data_chunk_to_arrow(ctx->arrow_options, chunk, out);
    duckdb_destroy_data_chunk(&chunk);
    return stream_check_error(ctx, error_data);
}

static const char *stream_get_last_error(struct ArrowArrayStream *stream) {
    arrowArrayStreamContext *ctx = (arrowArrayStreamContext *)stream->private_data;

    return ctx == NULL ? NULL : ctx->last_error;
}

static void stream_release(struct ArrowArrayStream *stream) {
    arrowArrayStreamContext *ctx;

    if (stream == NULL || stream->release == NULL) {
        return;
    }
    ctx = (arrowArrayStreamContext *)stream->private_data;
    if (ctx != NULL) {
        rbduckdb_result_unref(ctx->presult_ctx);
        if (ctx->arrow_options != NULL) {
            duckdb_destroy_arrow_options(&(ctx->arrow_options));
        }
        free(ctx->last_error);
        free(ctx);
    }
    stream->private_data = NULL;
    stream->release = NULL;
}

VALUE rbduckdb_create_arrow_array_stream(VALUE oDuckDBResult) {
    VALUE obj;
    rubyDuckDBArrowArrayStream *p;
    rubyDuckDBResult *presult_ctx;
    arrowArrayStreamContext *ctx;

    obj = allocate(cDuckDBArrowArrayStream);
    TypedData_Get_Struct(obj, rubyDuckDBArrowArrayStream, &arrow_array_stream_data_type, p);
    presult_ctx = rbduckdb_get_struct_result(oDuckDBResult);

    ctx = calloc((size_t)1, sizeof(arrowArrayStreamContext));
    if (ctx == NULL) {
        rb_raise(rb_eNoMemError, "failed to allocate ArrowArrayStream context");
    }

    rbduckdb_result_ref(presult_ctx);
    ctx->presult_ctx = presult_ctx;
    ctx->arrow_options = duckdb_result_get_arrow_options(&(presult_ctx->result));

    p->stream.get_schema = stream_get_schema;
    p->stream.get_next = stream_get_next;
    p->stream.get_last_error = stream_get_last_error;
    p->stream.release = stream_release;
    p->stream.private_data = ctx;

    return obj;
}

/*
 *  call-seq:
 *    stream.to_i -> Integer
 *
 *  Returns the address of the underlying C <code>struct ArrowArrayStream</code>.
 *  Arrow consumers such as red-arrow accept this address directly:
 *
 *    reader = Arrow::RecordBatchReader.import(stream.to_i)
 */
static VALUE arrow_array_stream_to_i(VALUE self) {
    rubyDuckDBArrowArrayStream *p;

    TypedData_Get_Struct(self, rubyDuckDBArrowArrayStream, &arrow_array_stream_data_type, p);
    return ULL2NUM((unsigned long long)(uintptr_t)&(p->stream));
}

/*
 *  call-seq:
 *    stream.arrow_c_stream -> self
 *
 *  Returns self. Defined so that the stream object itself satisfies the
 *  Arrow C stream protocol used by ruby-polars and others.
 */
static VALUE arrow_array_stream_arrow_c_stream(VALUE self) {
    return self;
}

void rbduckdb_init_arrow_array_stream(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBArrowArrayStream = rb_define_class_under(mDuckDB, "ArrowArrayStream", rb_cObject);

    rb_define_alloc_func(cDuckDBArrowArrayStream, allocate);

    rb_define_method(cDuckDBArrowArrayStream, "to_i", arrow_array_stream_to_i, 0);
    rb_define_method(cDuckDBArrowArrayStream, "arrow_c_stream", arrow_array_stream_arrow_c_stream, 0);
}
