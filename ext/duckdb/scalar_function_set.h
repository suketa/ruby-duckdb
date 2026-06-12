#ifndef RUBY_DUCKDB_SCALAR_FUNCTION_SET_H
#define RUBY_DUCKDB_SCALAR_FUNCTION_SET_H

struct _rubyDuckDBScalarFunctionSet {
    duckdb_scalar_function_set scalar_function_set;
    VALUE functions; /* Ruby Array of ScalarFunction objects — prevents GC collection */
};

typedef struct _rubyDuckDBScalarFunctionSet rubyDuckDBScalarFunctionSet;

void rbduckdb_init_scalar_function_set(void);
rubyDuckDBScalarFunctionSet *rbduckdb_get_struct_scalar_function_set(VALUE obj);

#endif
