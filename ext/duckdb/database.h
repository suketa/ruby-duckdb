#ifndef RUBY_DUCKDB_DATABASE_H
#define RUBY_DUCKDB_DATABASE_H

struct _rubyDuckDB {
    duckdb_database db;
    /* Functions and logical types registered on any connection to this database */
    VALUE registered_functions;
    /*
     * Set only for a database handed out by DuckDB::InstanceCache: the path it
     * was opened under, and the cache's weak set of live wrappers. Together they
     * let the cache hand back this same wrapper for the same path, and let
     * #close drop this wrapper from the set. Qnil for any other database.
     */
    VALUE cached_path;
    VALUE cache_wrappers;
};

typedef struct _rubyDuckDB rubyDuckDB;

rubyDuckDB *rbduckdb_get_struct_database(VALUE obj);
VALUE rbduckdb_create_database_obj(duckdb_database db);
void rbduckdb_database_retain(VALUE database, VALUE obj);
void rbduckdb_database_set_cache_entry(VALUE database, VALUE path, VALUE wrappers);
void rbduckdb_init_database(void);

#endif
