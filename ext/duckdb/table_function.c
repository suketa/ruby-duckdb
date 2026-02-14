#include "ruby-duckdb.h"

static VALUE cDuckDBTableFunction;
extern VALUE cDuckDBBindInfo;
extern VALUE cDuckDBInitInfo;
extern VALUE cDuckDBFunctionInfo;
extern VALUE cDuckDBDataChunk;

static void mark(void *ctx);
static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static void compact(void *ctx);
static VALUE duckdb_table_function_initialize(VALUE self);
static VALUE rbduckdb_table_function_set_name(VALUE self, VALUE name);
static VALUE rbduckdb_table_function_add_parameter(VALUE self, VALUE logical_type);
static VALUE rbduckdb_table_function_add_named_parameter(VALUE self, VALUE name, VALUE logical_type);
static VALUE rbduckdb_table_function_set_bind(VALUE self);
static void table_function_bind_callback(duckdb_bind_info info);
static VALUE rbduckdb_table_function_set_init(VALUE self);
static void table_function_init_callback(duckdb_init_info info);
static VALUE rbduckdb_table_function_set_execute(VALUE self);
static void table_function_execute_callback(duckdb_function_info info, duckdb_data_chunk output);

static const rb_data_type_t table_function_data_type = {
    "DuckDB/TableFunction",
    {mark, deallocate, memsize, compact},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void mark(void *ctx) {
    rubyDuckDBTableFunction *p = (rubyDuckDBTableFunction *)ctx;
    rb_gc_mark(p->bind_proc);
    rb_gc_mark(p->init_proc);
    rb_gc_mark(p->execute_proc);
}

static void deallocate(void *ctx) {
    rubyDuckDBTableFunction *p = (rubyDuckDBTableFunction *)ctx;

    if (p->table_function) {
        duckdb_destroy_table_function(&(p->table_function));
        p->table_function = NULL;
    }
    xfree(p);
}

/*
 * GC compaction callback - updates VALUE references that may have moved during compaction.
 * This is critical for Ruby 2.7+ where GC can move objects in memory.
 * TableFunction has three callback procs (bind, init, execute) that all need updating.
 * Without this, these VALUE pointers could become stale after compaction,
 * leading to crashes when DuckDB invokes the callbacks.
 */
static void compact(void *ctx) {
    rubyDuckDBTableFunction *p = (rubyDuckDBTableFunction *)ctx;
    if (p->bind_proc != Qnil) {
        p->bind_proc = rb_gc_location(p->bind_proc);
    }
    if (p->init_proc != Qnil) {
        p->init_proc = rb_gc_location(p->init_proc);
    }
    if (p->execute_proc != Qnil) {
        p->execute_proc = rb_gc_location(p->execute_proc);
    }
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBTableFunction *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBTableFunction));
    return TypedData_Wrap_Struct(klass, &table_function_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBTableFunction);
}

/*
 * call-seq:
 *   DuckDB::TableFunction.new -> DuckDB::TableFunction
 *
 * Creates a new table function.
 *
 *   tf = DuckDB::TableFunction.new
 *   tf.name = "my_function"
 *   # ... configure tf ...
 */
static VALUE duckdb_table_function_initialize(VALUE self) {
    rubyDuckDBTableFunction *ctx;

    TypedData_Get_Struct(self, rubyDuckDBTableFunction, &table_function_data_type, ctx);

    ctx->table_function = duckdb_create_table_function();
    if (!ctx->table_function) {
        rb_raise(eDuckDBError, "Failed to create table function");
    }

    ctx->bind_proc = Qnil;
    ctx->init_proc = Qnil;
    ctx->execute_proc = Qnil;

    // Set extra_info to the C struct pointer (safe with GC compaction)
    // Store ctx instead of self - ctx is xmalloc'd and won't move during GC
    duckdb_table_function_set_extra_info(ctx->table_function, ctx, NULL);

    return self;
}

/*
 * call-seq:
 *   table_function.name = name -> name
 *
 * Sets the name of the table function.
 *
 *   tf.name = "my_function"
 */
static VALUE rbduckdb_table_function_set_name(VALUE self, VALUE name) {
    rubyDuckDBTableFunction *ctx;
    const char *func_name;

    TypedData_Get_Struct(self, rubyDuckDBTableFunction, &table_function_data_type, ctx);

    if (!ctx->table_function) {
        rb_raise(eDuckDBError, "Table function is destroyed");
    }

    func_name = StringValueCStr(name);
    duckdb_table_function_set_name(ctx->table_function, func_name);

    return name;
}

/*
 * call-seq:
 *   table_function.add_parameter(logical_type) -> self
 *
 * Adds a positional parameter to the table function.
 *
 *   tf.add_parameter(DuckDB::LogicalType::BIGINT)
 *   tf.add_parameter(DuckDB::LogicalType::VARCHAR)
 */
static VALUE rbduckdb_table_function_add_parameter(VALUE self, VALUE logical_type) {
    rubyDuckDBTableFunction *ctx;
    rubyDuckDBLogicalType *ctx_logical_type;

    TypedData_Get_Struct(self, rubyDuckDBTableFunction, &table_function_data_type, ctx);

    if (!ctx->table_function) {
        rb_raise(eDuckDBError, "Table function is destroyed");
    }

    ctx_logical_type = get_struct_logical_type(logical_type);
    duckdb_table_function_add_parameter(ctx->table_function, ctx_logical_type->logical_type);

    return self;
}

/*
 * call-seq:
 *   table_function.add_named_parameter(name, logical_type) -> self
 *
 * Adds a named parameter to the table function.
 *
 *   tf.add_named_parameter("limit", DuckDB::LogicalType::BIGINT)
 */
static VALUE rbduckdb_table_function_add_named_parameter(VALUE self, VALUE name, VALUE logical_type) {
    rubyDuckDBTableFunction *ctx;
    rubyDuckDBLogicalType *ctx_logical_type;
    const char *param_name;

    TypedData_Get_Struct(self, rubyDuckDBTableFunction, &table_function_data_type, ctx);

    if (!ctx->table_function) {
        rb_raise(eDuckDBError, "Table function is destroyed");
    }

    param_name = StringValueCStr(name);
    ctx_logical_type = get_struct_logical_type(logical_type);
    duckdb_table_function_add_named_parameter(ctx->table_function, param_name, ctx_logical_type->logical_type);

    return self;
}

/*
 * call-seq:
 *   table_function.bind { |bind_info| ... } -> self
 *
 * Sets the bind callback for the table function.
 * The callback is called when the function is used in a query.
 *
 *   table_function.bind do |bind_info|
 *     bind_info.add_result_column('id', DuckDB::LogicalType::BIGINT)
 *     bind_info.add_result_column('name', DuckDB::LogicalType::VARCHAR)
 *   end
 */
static VALUE rbduckdb_table_function_set_bind(VALUE self) {
    rubyDuckDBTableFunction *ctx;

    if (!rb_block_given_p()) {
        rb_raise(rb_eArgError, "block is required");
    }

    TypedData_Get_Struct(self, rubyDuckDBTableFunction, &table_function_data_type, ctx);

    if (!ctx->table_function) {
        rb_raise(eDuckDBError, "Table function is destroyed");
    }

    ctx->bind_proc = rb_block_proc();

    duckdb_table_function_set_bind(ctx->table_function, table_function_bind_callback);

    return self;
}

static VALUE call_bind_proc(VALUE arg) {
    VALUE *args = (VALUE *)arg;
    return rb_funcall(args[0], rb_intern("call"), 1, args[1]);
}

static void table_function_bind_callback(duckdb_bind_info info) {
    rubyDuckDBTableFunction *ctx;
    rubyDuckDBBindInfo *bind_info_ctx;
    VALUE bind_info_obj;
    int state = 0;

    // Get the C struct pointer (safe with GC compaction)
    ctx = (rubyDuckDBTableFunction *)duckdb_bind_get_extra_info(info);
    if (!ctx || ctx->bind_proc == Qnil) {
        return;
    }

    // Create BindInfo wrapper
    bind_info_obj = rb_class_new_instance(0, NULL, cDuckDBBindInfo);
    bind_info_ctx = get_struct_bind_info(bind_info_obj);
    bind_info_ctx->bind_info = info;

    // Call Ruby block with exception protection
    VALUE call_args[2] = { ctx->bind_proc, bind_info_obj };
    rb_protect(call_bind_proc, (VALUE)call_args, &state);

    if (state) {
        VALUE err = rb_errinfo();
        VALUE msg = rb_funcall(err, rb_intern("message"), 0);
        duckdb_bind_set_error(info, StringValueCStr(msg));
        rb_set_errinfo(Qnil); // Clear the error
    }
}

/*
 * call-seq:
 *   table_function.init { |init_info| ... } -> table_function
 *
 * Sets the init callback for the table function.
 * The callback is invoked once during query initialization to set up execution state.
 *
 *   table_function.init do |init_info|
 *     # Initialize execution state
 *   end
 */
static VALUE rbduckdb_table_function_set_init(VALUE self) {
    rubyDuckDBTableFunction *ctx;

    if (!rb_block_given_p()) {
        rb_raise(rb_eArgError, "block is required for init");
    }

    TypedData_Get_Struct(self, rubyDuckDBTableFunction, &table_function_data_type, ctx);

    if (!ctx->table_function) {
        rb_raise(eDuckDBError, "Table function is destroyed");
    }

    ctx->init_proc = rb_block_proc();
    duckdb_table_function_set_init(ctx->table_function, table_function_init_callback);

    return self;
}

static VALUE call_init_proc(VALUE args_val) {
    VALUE *args = (VALUE *)args_val;
    return rb_funcall(args[0], rb_intern("call"), 1, args[1]);
}

static void table_function_init_callback(duckdb_init_info info) {
    rubyDuckDBTableFunction *ctx;
    VALUE init_info_obj;
    rubyDuckDBInitInfo *init_info_ctx;
    int state = 0;

    // Get the C struct pointer (safe with GC compaction)
    ctx = (rubyDuckDBTableFunction *)duckdb_init_get_extra_info(info);
    if (!ctx || ctx->init_proc == Qnil) {
        return;
    }

    // Create InitInfo wrapper
    init_info_obj = rb_class_new_instance(0, NULL, cDuckDBInitInfo);
    init_info_ctx = get_struct_init_info(init_info_obj);
    init_info_ctx->info = info;

    // Call Ruby block with exception protection
    VALUE call_args[2] = { ctx->init_proc, init_info_obj };
    rb_protect(call_init_proc, (VALUE)call_args, &state);

    if (state) {
        VALUE err = rb_errinfo();
        VALUE msg = rb_funcall(err, rb_intern("message"), 0);
        duckdb_init_set_error(info, StringValueCStr(msg));
        rb_set_errinfo(Qnil); // Clear the error
    }
}

/*
 * call-seq:
 *   table_function.execute { |function_info, output| ... } -> table_function
 *
 * Sets the execute callback for the table function.
 * The callback is invoked during query execution to generate output rows.
 *
 *   table_function.execute do |func_info, output|
 *     output.size = 10
 *     vec = output.get_vector(0)
 *     # Write data...
 *   end
 */
static VALUE rbduckdb_table_function_set_execute(VALUE self) {
    rubyDuckDBTableFunction *ctx;

    if (!rb_block_given_p()) {
        rb_raise(rb_eArgError, "block is required for execute");
    }

    TypedData_Get_Struct(self, rubyDuckDBTableFunction, &table_function_data_type, ctx);

    ctx->execute_proc = rb_block_proc();
    duckdb_table_function_set_function(ctx->table_function, table_function_execute_callback);

    return self;
}

static VALUE call_execute_proc(VALUE args_val) {
    VALUE *args = (VALUE *)args_val;
    return rb_funcall(args[0], rb_intern("call"), 2, args[1], args[2]);
}

static void table_function_execute_callback(duckdb_function_info info, duckdb_data_chunk output) {
    rubyDuckDBTableFunction *ctx;
    VALUE func_info_obj;
    VALUE data_chunk_obj;
    rubyDuckDBFunctionInfo *func_info_ctx;
    rubyDuckDBDataChunk *data_chunk_ctx;
    int state = 0;

    // Get the C struct pointer (safe with GC compaction)
    ctx = (rubyDuckDBTableFunction *)duckdb_function_get_extra_info(info);
    if (!ctx || ctx->execute_proc == Qnil) {
        return;
    }

    // Create FunctionInfo wrapper
    func_info_obj = rb_class_new_instance(0, NULL, cDuckDBFunctionInfo);
    func_info_ctx = get_struct_function_info(func_info_obj);
    func_info_ctx->info = info;

    // Create DataChunk wrapper
    data_chunk_obj = rb_class_new_instance(0, NULL, cDuckDBDataChunk);
    data_chunk_ctx = get_struct_data_chunk(data_chunk_obj);
    data_chunk_ctx->data_chunk = output;

    // Call Ruby block with exception protection
    VALUE call_args[3] = { ctx->execute_proc, func_info_obj, data_chunk_obj };
    rb_protect(call_execute_proc, (VALUE)call_args, &state);

    if (state) {
        VALUE err = rb_errinfo();
        VALUE msg = rb_funcall(err, rb_intern("message"), 0);
        duckdb_function_set_error(info, StringValueCStr(msg));
        rb_set_errinfo(Qnil); // Clear the error
    }
}

rubyDuckDBTableFunction *get_struct_table_function(VALUE self) {
    rubyDuckDBTableFunction *ctx;
    TypedData_Get_Struct(self, rubyDuckDBTableFunction, &table_function_data_type, ctx);
    return ctx;
}

void rbduckdb_init_duckdb_table_function(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBTableFunction = rb_define_class_under(mDuckDB, "TableFunction", rb_cObject);
    rb_define_alloc_func(cDuckDBTableFunction, allocate);

    rb_define_method(cDuckDBTableFunction, "initialize", duckdb_table_function_initialize, 0);
    rb_define_method(cDuckDBTableFunction, "set_name", rbduckdb_table_function_set_name, 1);
    rb_define_method(cDuckDBTableFunction, "name=", rbduckdb_table_function_set_name, 1);
    rb_define_method(cDuckDBTableFunction, "add_parameter", rbduckdb_table_function_add_parameter, 1);
    rb_define_method(cDuckDBTableFunction, "add_named_parameter", rbduckdb_table_function_add_named_parameter, 2);
    rb_define_method(cDuckDBTableFunction, "bind", rbduckdb_table_function_set_bind, 0);
    rb_define_method(cDuckDBTableFunction, "init", rbduckdb_table_function_set_init, 0);
    rb_define_method(cDuckDBTableFunction, "execute", rbduckdb_table_function_set_execute, 0);
}
