#ifndef RUBY_DUCKDB_LOGICAL_TYPE_H
#define RUBY_DUCKDB_LOGICAL_TYPE_H

struct _rubyDuckDBLogicalType {
    duckdb_logical_type logical_type;
};

typedef struct _rubyDuckDBLogicalType rubyDuckDBLogicalType;

void rbduckdb_init_duckdb_logical_type(void);
#endif
