#ifndef RUBY_DUCKDB_H
#define RUBY_DUCKDB_H

#include "ruby.h"
#include <duckdb.h>
#include "./error.h"
#include "./database.h"
#include "./connection.h"
#include "./result.h"
#include "./prepared_statement.h"

extern VALUE mDuckDB;
extern VALUE cDuckDBDatabase;
extern VALUE cDuckDBConnection;
extern VALUE eDuckDBError;

#endif
