#ifndef RUBY_DUCKDB_INSTANCE_CACHE_H
#define RUBY_DUCKDB_INSTANCE_CACHE_H

struct _rubyDuckDBInstanceCache {
    duckdb_instance_cache instance_cache;
    /*
     * ObjectSpace::WeakMap used as a weak set of the DuckDB::Database wrappers
     * this cache has handed out for file paths. Weak so a wrapper nobody holds
     * is still collected, and its DuckDB instance closed, as before.
     */
    VALUE wrappers;
};

typedef struct _rubyDuckDBInstanceCache rubyDuckDBInstanceCache;

void rbduckdb_init_instance_cache(void);

#endif
