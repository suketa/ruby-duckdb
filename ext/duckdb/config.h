#ifndef RUBY_DUCKDB_CONFIG_H
#define RUBY_DUCKDB_CONFIG_H

struct _rubyDuckDBConfig {
    duckdb_config config;
};

typedef struct _rubyDuckDBConfig rubyDuckDBConfig;

rubyDuckDBConfig *get_struct_config(VALUE obj);

void init_duckdb_config(void);

#endif
