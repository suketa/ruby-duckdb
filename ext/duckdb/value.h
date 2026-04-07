#ifndef RUBY_DUCKDB_VALUE_H
#define RUBY_DUCKDB_VALUE_H

struct _rubyDuckDBValue {
    duckdb_value value;
};

typedef struct _rubyDuckDBValue rubyDuckDBValue;

void rbduckdb_init_duckdb_value(void);
VALUE rbduckdb_value_new(duckdb_value value);
VALUE rbduckdb_duckdb_value_to_ruby(duckdb_value val);
rubyDuckDBValue *get_struct_value(VALUE obj);

#endif
