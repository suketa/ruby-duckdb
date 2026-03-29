#ifndef RUBY_DUCKDB_SCALAR_FUNCTION_SET_H
#define RUBY_DUCKDB_SCALAR_FUNCTION_SET_H

struct _rubyDuckDBScalarFunctionSet {
    duckdb_scalar_function_set scalar_function_set;
};

typedef struct _rubyDuckDBScalarFunctionSet rubyDuckDBScalarFunctionSet;

void rbduckdb_init_duckdb_scalar_function_set(void);
rubyDuckDBScalarFunctionSet *get_struct_scalar_function_set(VALUE obj);

#endif
