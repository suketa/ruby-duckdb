#ifndef RUBY_DUCKDB_LOGICAL_TYPE_H
#define RUBY_DUCKDB_LOGICAL_TYPE_H

struct _rubyDuckDBLogicalType {
    duckdb_logical_type logical_type;
};

typedef struct _rubyDuckDBLogicalType rubyDuckDBLogicalType;

void rbduckdb_init_duckdb_logical_type(void);
VALUE rbduckdb_create_logical_type(duckdb_logical_type logical_type);
rubyDuckDBLogicalType *get_struct_logical_type(VALUE obj);
#endif
