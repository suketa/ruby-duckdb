#ifndef RUBY_DUCKDB_TABLE_FUNCTION_INIT_INFO_H
#define RUBY_DUCKDB_TABLE_FUNCTION_INIT_INFO_H

struct _rubyDuckDBInitInfo {
    duckdb_init_info info;
};

typedef struct _rubyDuckDBInitInfo rubyDuckDBInitInfo;

extern VALUE cDuckDBTableFunctionInitInfo;
rubyDuckDBInitInfo *get_struct_init_info(VALUE obj);
void rbduckdb_init_duckdb_init_info(void);

#endif
