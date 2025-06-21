#ifndef RUBY_DUCKDB_INSTANCE_CACHE_H
#define RUBY_DUCKDB_INSTANCE_CACHE_H

struct _rubyDuckDBInstanceCache {
    duckdb_instance_cache instance_cache;
};

typedef struct _rubyDuckDBInstanceCache rubyDuckDBInstanceCache;

void rbduckdb_init_duckdb_instance_cache(void);

#endif
