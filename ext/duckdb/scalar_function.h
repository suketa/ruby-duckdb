#ifndef RUBY_DUCKDB_SCALAR_FUNCTION_H
#define RUBY_DUCKDB_SCALAR_FUNCTION_H

struct _rubyDuckDBScalarFunction {
    duckdb_scalar_function scalar_function;
};

typedef struct _rubyDuckDBScalarFunction rubyDuckDBScalarFunction;

void rbduckdb_init_duckdb_scalar_function(void);

#endif


