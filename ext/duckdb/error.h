#ifndef RUBY_DUCKDB_ERROR_H
#define RUBY_DUCKDB_ERROR_H

void rbduckdb_init_error(void);
NORETURN(void rbduckdb_raise_result_error(duckdb_result *result));

#endif
