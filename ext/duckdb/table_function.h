#ifndef RUBY_DUCKDB_TABLE_FUNCTION_H
#define RUBY_DUCKDB_TABLE_FUNCTION_H

struct _rubyDuckDBTableFunction {
    duckdb_table_function table_function;
    VALUE bind_proc;
    VALUE init_proc;
    VALUE execute_proc;
};

typedef struct _rubyDuckDBTableFunction rubyDuckDBTableFunction;

rubyDuckDBTableFunction *get_struct_table_function(VALUE self);
void rbduckdb_init_duckdb_table_function(void);

#endif
