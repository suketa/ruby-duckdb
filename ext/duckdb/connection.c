#include "ruby-duckdb.h"

VALUE cDuckDBConnection;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE duckdb_connection_disconnect(VALUE self);
static VALUE duckdb_connection_interrupt(VALUE self);
static VALUE duckdb_connection_query_progress(VALUE self);
static VALUE duckdb_connection_connect(VALUE self, VALUE oDuckDBDatabase);
static VALUE duckdb_connection_query_sql(VALUE self, VALUE str);
static VALUE duckdb_connection_register_scalar_function(VALUE self, VALUE funcDef);

static const rb_data_type_t connection_data_type = {
    "DuckDB/Connection",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBConnection *p = (rubyDuckDBConnection *)ctx;

    duckdb_disconnect(&(p->con));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBConnection *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBConnection));
    return TypedData_Wrap_Struct(klass, &connection_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBConnection);
}

rubyDuckDBConnection *get_struct_connection(VALUE obj) {
    rubyDuckDBConnection *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBConnection, &connection_data_type, ctx);
    return ctx;
}

VALUE rbduckdb_create_connection(VALUE oDuckDBDatabase) {
    rubyDuckDB *ctxdb;
    rubyDuckDBConnection *ctxcon;
    VALUE obj;

    ctxdb = rbduckdb_get_struct_database(oDuckDBDatabase);

    obj = allocate(cDuckDBConnection);
    TypedData_Get_Struct(obj, rubyDuckDBConnection, &connection_data_type, ctxcon);

    if (duckdb_connect(ctxdb->db, &(ctxcon->con)) == DuckDBError) {
        rb_raise(eDuckDBError, "connection error");
    }

    return obj;
}

static VALUE duckdb_connection_disconnect(VALUE self) {
    rubyDuckDBConnection *ctx;

    TypedData_Get_Struct(self, rubyDuckDBConnection, &connection_data_type, ctx);
    duckdb_disconnect(&(ctx->con));

    return self;
}

/*
 * call-seq:
 *   connection.interrupt -> nil
 *
 * Interrupts the currently running query.
 *
 *  db = DuckDB::Database.open
 *  conn = db.connect
 *  con.query('SET ENABLE_PROGRESS_BAR=true')
 *  con.query('SET ENABLE_PROGRESS_BAR_PRINT=false')
 *  pending_result = con.async_query('slow query')
 *
 *  pending_result.execute_task
 *  con.interrupt # => nil
 */
static VALUE duckdb_connection_interrupt(VALUE self) {
    rubyDuckDBConnection *ctx;

    TypedData_Get_Struct(self, rubyDuckDBConnection, &connection_data_type, ctx);
    duckdb_interrupt(ctx->con);

    return Qnil;
}

/*
 * Returns the progress of the currently running query.
 *
 *  require 'duckdb'
 *
 *  db = DuckDB::Database.open
 *  conn = db.connect
 *  con.query('SET ENABLE_PROGRESS_BAR=true')
 *  con.query('SET ENABLE_PROGRESS_BAR_PRINT=false')
 *  con.query_progress # => -1.0
 *  pending_result = con.async_query('slow query')
 *  con.query_progress # => 0.0
 *  pending_result.execute_task
 *  con.query_progress # => Float
 */
static VALUE duckdb_connection_query_progress(VALUE self) {
    rubyDuckDBConnection *ctx;
    duckdb_query_progress_type progress;

    TypedData_Get_Struct(self, rubyDuckDBConnection, &connection_data_type, ctx);
    progress = duckdb_query_progress(ctx->con);

    return rb_funcall(mDuckDBConverter, rb_intern("_to_query_progress"), 3, DBL2NUM(progress.percentage), ULL2NUM(progress.rows_processed), ULL2NUM(progress.total_rows_to_process));
}

/* :nodoc: */
static VALUE duckdb_connection_connect(VALUE self, VALUE oDuckDBDatabase) {
    rubyDuckDBConnection *ctx;
    rubyDuckDB *ctxdb;

    if (!rb_obj_is_kind_of(oDuckDBDatabase, cDuckDBDatabase)) {
        rb_raise(rb_eTypeError, "The first argument must be DuckDB::Database object.");
    }
    ctxdb = rbduckdb_get_struct_database(oDuckDBDatabase);
    TypedData_Get_Struct(self, rubyDuckDBConnection, &connection_data_type, ctx);

    if (duckdb_connect(ctxdb->db, &(ctx->con)) == DuckDBError) {
        rb_raise(eDuckDBError, "connection error");
    }

    return self;
}

/* :nodoc: */
static VALUE duckdb_connection_query_sql(VALUE self, VALUE str) {
    rubyDuckDBConnection *ctx;
    rubyDuckDBResult *ctxr;

    VALUE result = rbduckdb_create_result();

    TypedData_Get_Struct(self, rubyDuckDBConnection, &connection_data_type, ctx);
    ctxr = get_struct_result(result);

    if (!(ctx->con)) {
        rb_raise(eDuckDBError, "Database connection closed");
    }

    if (duckdb_query(ctx->con, StringValueCStr(str), &(ctxr->result)) == DuckDBError) {
        rb_raise(eDuckDBError, "%s", duckdb_result_error(&(ctxr->result)));
    }
    return result;
}

typedef struct {
    VALUE rb_impl_val;
    duckdb_type duckdb_return_type_id;
    long duckdb_parameter_len;
    duckdb_type *duckdb_parameter_type_ids;
} scalar_function_impl_wrapper_extra_info;

static const int MAX_SCALAR_FUNCTION_PARAMETERS = 16;

/* :nodoc: */
static VALUE scalar_function_impl_wrapper_impl(VALUE args) {
    VALUE *argv = (VALUE*)args;
    VALUE recv = argv[0];
    ID mid = (ID)argv[1];
    int argc = (int)argv[2];
    VALUE *func_args = (VALUE*)argv[3];

    return rb_funcallv(recv, mid, argc, func_args);
}

/* :nodoc: */
static void scalar_function_impl_wrapper(duckdb_function_info function_info, duckdb_data_chunk input, duckdb_vector output) {
	idx_t chunkSize = duckdb_data_chunk_get_size(input);

    scalar_function_impl_wrapper_extra_info *extra_info = duckdb_scalar_function_get_extra_info(function_info);

    VALUE ruby_call_args[MAX_SCALAR_FUNCTION_PARAMETERS];
    ID call_kw = rb_intern("call");

    void *input_vectors_data[MAX_SCALAR_FUNCTION_PARAMETERS];
    for (long j = 0; j < extra_info->duckdb_parameter_len; j++) {
        input_vectors_data[j] = duckdb_vector_get_data(duckdb_data_chunk_get_vector(input, j));
    }

    void *output_vector_data = duckdb_vector_get_data(output);

	for(idx_t i = 0; i < chunkSize; i++) {
	    // Extract parameters
	    for (long j = 0; j < extra_info->duckdb_parameter_len; j++) {
            // TODO: extract in its own function
            switch(extra_info->duckdb_parameter_type_ids[j]) {
                case DUCKDB_TYPE_VARCHAR: {
                    duckdb_string_t duckdb_string = ((duckdb_string_t*)(input_vectors_data[j]))[i];
                    const char *c_string = duckdb_string_t_data(&duckdb_string);
                    VALUE ruby_string = rb_str_new_cstr(c_string);
                    ruby_call_args[j] = ruby_string;
                    break;
                }
                case DUCKDB_TYPE_INTEGER: {
                    int32_t value = ((int32_t*)(input_vectors_data[j]))[i];
                    ruby_call_args[j] = INT2NUM(value);
                    break;
                }
                default: {
                    // TODO: add `j` and `extra_info->duckdb_parameter_type_ids[j]` in error log
                    duckdb_scalar_function_set_error(
                        function_info,
                        "Internal ruby-duckdb error: unexpected duckdb_type while handling parameter"
                    );
                    return;
                }
            }
	    }

	    int ruby_call_error;
        VALUE args[4] = {
            extra_info->rb_impl_val,
            (VALUE)call_kw,
            (VALUE)((int)(extra_info->duckdb_parameter_len)),
            (VALUE)ruby_call_args
        };
        VALUE ruby_result_val = rb_protect(scalar_function_impl_wrapper_impl, (VALUE)args, &ruby_call_error);

        if (ruby_call_error) {
            VALUE ruby_err = rb_errinfo();
            VALUE ruby_err_msg = rb_funcall(ruby_err, rb_intern("message"), 0);
            const char* ruby_err_msg_cstr = StringValueCStr(ruby_err_msg);

            const char* duckdb_error_msg_prefix = "Ruby error raise while executing the UDF: ";
            char* duckdb_error_msg = malloc(strlen(duckdb_error_msg_prefix) + strlen(ruby_err_msg_cstr) + 1);
            strcpy(duckdb_error_msg, duckdb_error_msg_prefix);
            strcat(duckdb_error_msg, ruby_err_msg_cstr);

            duckdb_scalar_function_set_error(function_info, duckdb_error_msg);
            return;
        }

        if (NIL_P(ruby_result_val)) {
            duckdb_vector_ensure_validity_writable(output);

            uint64_t *validity_mask = duckdb_vector_get_validity(output);
            duckdb_validity_set_row_invalid(validity_mask, i);
            return;
        }

        // Convert result and store in output vector
        // TODO: extract in its own function
        switch(extra_info->duckdb_return_type_id) {
            case DUCKDB_TYPE_VARCHAR: {
                if (!RB_TYPE_P(ruby_result_val, T_STRING)) {
                    duckdb_scalar_function_set_error(function_info, "Returned value from UDF is not a text");
                }
                else {
                    duckdb_vector_assign_string_element(output, i, StringValueCStr(ruby_result_val));
                }
                break;
            }
            case DUCKDB_TYPE_INTEGER: {
                if (!RB_TYPE_P(ruby_result_val, T_FIXNUM)) {
                    duckdb_scalar_function_set_error(function_info, "Returned value from UDF is not an integer");
                }
                else {
                    int32_t *result_data = output_vector_data;
                    result_data[i] = NUM2INT(ruby_result_val);
                }
                break;
            }
            default: {
                // TODO: add `extra_info->duckdb_return_type_id` in error log
                duckdb_scalar_function_set_error(
                    function_info,
                    "Internal ruby-duckdb error: unexpected duckdb_type while setting output value"
                );
            }
        }
	}
}

/* :nodoc: */
static duckdb_logical_type sym_to_duckdb_logical_type(VALUE sym) {
    if (SYM2ID(sym) == rb_intern("text")) {
        return duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    }
    else if (SYM2ID(sym) == rb_intern("integer")) {
        return duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    }
    else {
        // TODO: better name
        rb_raise(rb_eRuntimeError, "Unknown DuckDB logical type for the symbol");
    }
}

/* :nodoc: */
static void scalar_function_extra_info_delete_callback(void *raw_extra_info) {
    scalar_function_impl_wrapper_extra_info *extra_info = raw_extra_info;

    free(extra_info->duckdb_parameter_type_ids);
    free(raw_extra_info);
}

/* :nodoc: */
static VALUE duckdb_connection_register_scalar_function(VALUE self, VALUE funcDef) {
    // Inspired from https://github.com/duckdb/duckdb/blob/8a67a5450dad6b33709b16037f775e800b147ed9/extension/demo_capi/add_numbers.cpp

    rubyDuckDBConnection *ctx;

    TypedData_Get_Struct(self, rubyDuckDBConnection, &connection_data_type, ctx);

    VALUE scalarFuncNameVal = rb_hash_aref(funcDef, ID2SYM(rb_intern("name")));
    VALUE scalarFuncImplVal = rb_hash_aref(funcDef, ID2SYM(rb_intern("impl")));
    VALUE scalarFuncReturnTypeVal = rb_hash_aref(funcDef, ID2SYM(rb_intern("return_type")));
    VALUE scalarFuncParameterTypesVal = rb_hash_aref(funcDef, ID2SYM(rb_intern("parameter_types")));
    VALUE scalarFuncVolatileVal = rb_hash_aref(funcDef, ID2SYM(rb_intern("volatile")));

    duckdb_scalar_function scalarFunc = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(scalarFunc, StringValueCStr(scalarFuncNameVal));
    duckdb_scalar_function_set_function(scalarFunc, scalar_function_impl_wrapper);

    // Set volatile if required
    if (scalarFuncVolatileVal == Qtrue) {
        duckdb_scalar_function_set_volatile(scalarFunc);
    }

    // Set return type
    duckdb_logical_type return_type = sym_to_duckdb_logical_type(scalarFuncReturnTypeVal);
    duckdb_scalar_function_set_return_type(scalarFunc, return_type);

    // Set extra information
    scalar_function_impl_wrapper_extra_info *extra_info = malloc(sizeof(scalar_function_impl_wrapper_extra_info));
    extra_info->rb_impl_val = scalarFuncImplVal;
    // Is `rb_gc_mark(extra_info->rb_impl_val)` needed?
    extra_info->duckdb_return_type_id = duckdb_get_type_id(return_type);
    duckdb_scalar_function_set_extra_info(scalarFunc, extra_info, scalar_function_extra_info_delete_callback);

    duckdb_destroy_logical_type(&return_type);

    // Set parameters
    long parameterTypesLen = RARRAY_LEN(scalarFuncParameterTypesVal);
    if (parameterTypesLen > MAX_SCALAR_FUNCTION_PARAMETERS) {
        rb_raise(rb_eTypeError, "Too much parameters added to the scalar function (ruby-duckdb internal limit)");
    }

    extra_info->duckdb_parameter_len = parameterTypesLen;
    extra_info->duckdb_parameter_type_ids = malloc(parameterTypesLen * sizeof(duckdb_type));
    for (long i = 0; i < parameterTypesLen; i++) {
        VALUE scalarFuncParameterTypeIVal = RARRAY_AREF(scalarFuncParameterTypesVal, i);
        duckdb_logical_type parameter_type_i = sym_to_duckdb_logical_type(scalarFuncParameterTypeIVal);
        duckdb_scalar_function_add_parameter(scalarFunc, parameter_type_i);
        extra_info->duckdb_parameter_type_ids[i] = duckdb_get_type_id(parameter_type_i);

        duckdb_destroy_logical_type(&parameter_type_i);
    }

    // Register function
    if (duckdb_register_scalar_function(ctx->con, scalarFunc) != DuckDBSuccess) {
        rb_raise(rb_eTypeError, "Unable to register scalar function");
    }

    duckdb_destroy_scalar_function(&scalarFunc);

    return Qnil;
}

void rbduckdb_init_duckdb_connection(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBConnection = rb_define_class_under(mDuckDB, "Connection", rb_cObject);
    rb_define_alloc_func(cDuckDBConnection, allocate);

    rb_define_method(cDuckDBConnection, "disconnect", duckdb_connection_disconnect, 0);
    rb_define_method(cDuckDBConnection, "interrupt", duckdb_connection_interrupt, 0);
    rb_define_method(cDuckDBConnection, "query_progress", duckdb_connection_query_progress, 0);
    rb_define_private_method(cDuckDBConnection, "_connect", duckdb_connection_connect, 1);
    /* TODO: query_sql => _query_sql */
    rb_define_private_method(cDuckDBConnection, "query_sql", duckdb_connection_query_sql, 1);
    rb_define_private_method(cDuckDBConnection, "_register_scalar_function", duckdb_connection_register_scalar_function, 1);
}
