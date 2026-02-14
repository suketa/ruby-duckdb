#include "ruby-duckdb.h"

static VALUE cDuckDBTableFunction;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE duckdb_table_function_s_create(VALUE self);
static VALUE duckdb_table_function_destroy(VALUE self);
static VALUE rbduckdb_table_function_set_name(VALUE self, VALUE name);
static VALUE rbduckdb_table_function_add_parameter(VALUE self, VALUE logical_type);
static VALUE rbduckdb_table_function_add_named_parameter(VALUE self, VALUE name, VALUE logical_type);

static const rb_data_type_t table_function_data_type = {
    "DuckDB/TableFunction",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBTableFunction *p = (rubyDuckDBTableFunction *)ctx;
    
    if (p->table_function) {
        duckdb_destroy_table_function(&(p->table_function));
        p->table_function = NULL;
    }
    xfree(p);
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
 *   DuckDB::TableFunction.create -> DuckDB::TableFunction
 *   DuckDB::TableFunction.create { |tf| ... } -> result
 *
 * Creates a new table function.
 * If a block is given, the table function is yielded and automatically destroyed.
 *
 *   tf = DuckDB::TableFunction.create
 *   tf.name = "my_function"
 *   # ... configure tf ...
 *   tf.destroy
 *
 *   # Or with block:
 *   DuckDB::TableFunction.create do |tf|
 *     tf.name = "my_function"
 *     # ... configure tf ...
 *   end
 */
static VALUE duckdb_table_function_s_create(VALUE self) {
    VALUE obj = allocate(cDuckDBTableFunction);
    rubyDuckDBTableFunction *ctx;
    
    TypedData_Get_Struct(obj, rubyDuckDBTableFunction, &table_function_data_type, ctx);
    
    ctx->table_function = duckdb_create_table_function();
    if (!ctx->table_function) {
        rb_raise(eDuckDBError, "Failed to create table function");
    }
    
    if (rb_block_given_p()) {
        return rb_ensure(rb_yield, obj, duckdb_table_function_destroy, obj);
    }
    
    return obj;
}

/*
 * call-seq:
 *   table_function.destroy -> nil
 *
 * Destroys the table function and releases its resources.
 * Safe to call multiple times.
 */
static VALUE duckdb_table_function_destroy(VALUE self) {
    rubyDuckDBTableFunction *ctx;
    TypedData_Get_Struct(self, rubyDuckDBTableFunction, &table_function_data_type, ctx);
    
    if (ctx->table_function) {
        duckdb_destroy_table_function(&(ctx->table_function));
        ctx->table_function = NULL;
    }
    
    return Qnil;
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

rubyDuckDBTableFunction *get_struct_table_function(VALUE self) {
    rubyDuckDBTableFunction *ctx;
    TypedData_Get_Struct(self, rubyDuckDBTableFunction, &table_function_data_type, ctx);
    return ctx;
}

void rbduckdb_init_duckdb_table_function(void) {
    cDuckDBTableFunction = rb_define_class_under(mDuckDB, "TableFunction", rb_cObject);
    
    rb_define_alloc_func(cDuckDBTableFunction, allocate);
    
    rb_define_singleton_method(cDuckDBTableFunction, "create", duckdb_table_function_s_create, 0);
    rb_define_method(cDuckDBTableFunction, "destroy", duckdb_table_function_destroy, 0);
    rb_define_method(cDuckDBTableFunction, "name=", rbduckdb_table_function_set_name, 1);
    rb_define_method(cDuckDBTableFunction, "add_parameter", rbduckdb_table_function_add_parameter, 1);
    rb_define_method(cDuckDBTableFunction, "add_named_parameter", rbduckdb_table_function_add_named_parameter, 2);
}
