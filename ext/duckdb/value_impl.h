#ifndef RUBY_DUCKDB_VALUE_IMPL_H
#define RUBY_DUCKDB_VALUE_IMPL_H

struct _rubyDuckDBValueImpl {
    duckdb_value value;
};

typedef struct _rubyDuckDBValueImpl rubyDuckDBValueImpl;

void rbduckdb_init_duckdb_value_impl(void);
VALUE rbduckdb_value_impl_new(duckdb_value value);
VALUE rbduckdb_duckdb_value_to_ruby(duckdb_value val);

#endif

