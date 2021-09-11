#ifndef RUBY_DUCKDB_CONFIG_H
#define RUBY_DUCKDB_CONFIG_H

#ifdef HAVE_DUCKDB_CREATE_CONFIG

struct _rubyDuckDBConfig {
    duckdb_config config;
};

typedef struct _rubyDuckDBConfig rubyDuckDBConfig;

void init_duckdb_config(void);

#endif

#endif

