#ifndef RUBY_DUCKDB_SCALAR_FUNCTION_BIND_INFO_H
#define RUBY_DUCKDB_SCALAR_FUNCTION_BIND_INFO_H

struct _rubyDuckDBScalarFunctionBindInfo {
    duckdb_bind_info bind_info;
};

typedef struct _rubyDuckDBScalarFunctionBindInfo rubyDuckDBScalarFunctionBindInfo;

void rbduckdb_init_duckdb_scalar_function_bind_info(void);
VALUE rbduckdb_scalar_function_bind_info_new(duckdb_bind_info bind_info);

#endif
