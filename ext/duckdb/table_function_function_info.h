#ifndef RUBY_DUCKDB_TABLE_FUNCTION_FUNCTION_INFO_H
#define RUBY_DUCKDB_TABLE_FUNCTION_FUNCTION_INFO_H

struct _rubyDuckDBFunctionInfo {
    duckdb_function_info info;
};

typedef struct _rubyDuckDBFunctionInfo rubyDuckDBFunctionInfo;

rubyDuckDBFunctionInfo *get_struct_function_info(VALUE obj);
void rbduckdb_init_duckdb_table_function_function_info(void);

#endif
