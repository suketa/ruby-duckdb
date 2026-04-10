#ifndef RUBY_DUCKDB_AGGREGATE_FUNCTION_H
#define RUBY_DUCKDB_AGGREGATE_FUNCTION_H

struct _rubyDuckDBAggregateFunction {
    duckdb_aggregate_function aggregate_function;
    VALUE init_proc;
    VALUE update_proc;
    VALUE finalize_proc;
};

typedef struct _rubyDuckDBAggregateFunction rubyDuckDBAggregateFunction;

void rbduckdb_init_duckdb_aggregate_function(void);
rubyDuckDBAggregateFunction *get_struct_aggregate_function(VALUE obj);

#endif
