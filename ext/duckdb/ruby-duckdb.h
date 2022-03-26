#ifndef RUBY_DUCKDB_H
#define RUBY_DUCKDB_H

#include "ruby.h"
#include <duckdb.h>
#include "./error.h"
#include "./database.h"
#include "./connection.h"
#include "./result.h"
#include "./column.h"
#include "./prepared_statement.h"
#include "./util.h"

#include "./blob.h"
#include "./appender.h"

#ifdef HAVE_DUCKDB_CREATE_CONFIG

#include "./config.h"

#endif /* HAVE_DUCKDB_CREATE_CONFIG */

extern VALUE mDuckDB;
extern VALUE cDuckDBDatabase;
extern VALUE cDuckDBConnection;
extern VALUE cDuckDBBlob;
extern VALUE cDuckDBConfig;
extern VALUE eDuckDBError;

#endif
