#ifndef RUBY_DUCKDB_TABLE_FUNCTION_BIND_INFO_H
#define RUBY_DUCKDB_TABLE_FUNCTION_BIND_INFO_H

struct _rubyDuckDBBindInfo {
    duckdb_bind_info bind_info;
};

typedef struct _rubyDuckDBBindInfo rubyDuckDBBindInfo;

extern VALUE cDuckDBTableFunctionBindInfo;
rubyDuckDBBindInfo *get_struct_bind_info(VALUE obj);
void rbduckdb_init_duckdb_table_function_bind_info(void);

#endif
