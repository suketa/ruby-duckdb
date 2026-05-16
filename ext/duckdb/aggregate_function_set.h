#ifndef RUBY_DUCKDB_AGGREGATE_FUNCTION_SET_H
#define RUBY_DUCKDB_AGGREGATE_FUNCTION_SET_H

struct _rubyDuckDBAggregateFunctionSet {
    duckdb_aggregate_function_set aggregate_function_set;
    VALUE functions; /* Ruby Array of AggregateFunction objects — prevents GC collection */
};

typedef struct _rubyDuckDBAggregateFunctionSet rubyDuckDBAggregateFunctionSet;

void rbduckdb_init_duckdb_aggregate_function_set(void);
rubyDuckDBAggregateFunctionSet *rbduckdb_get_struct_aggregate_function_set(VALUE obj);

#endif
