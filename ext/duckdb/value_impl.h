#ifndef RUBY_DUCKDB_VALUE_IMPL_H
#define RUBY_DUCKDB_VALUE_IMPL_H

struct _rubyDuckDBValueImpl {
    duckdb_value value;
};

typedef struct _rubyDuckDBValueImpl rubyDuckDBValueImpl;

void rbduckdb_init_duckdb_value_impl(void);

#endif

