#ifndef RUBY_DUCKDB_INSTANCE_CACHE_H
#define RUBY_DUCKDB_INSTANCE_CACHE_H

#ifdef HAVE_DUCKDB_H_GE_V1_2_0

struct _rubyDuckDBInstanceCache {
    duckdb_instance_cache instance_cache;
};

typedef struct _rubyDuckDBInstanceCache rubyDuckDBInstanceCache;

void rbduckdb_init_duckdb_instance_cache(void);

#endif

#endif

