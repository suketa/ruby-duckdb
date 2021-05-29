#ifndef RUBY_DUCKDB_H
#define RUBY_DUCKDB_H

#include "ruby.h"
#include <duckdb.h>
#include "./error.h"
#include "./database.h"
#include "./connection.h"
#include "./result.h"
#include "./prepared_statement.h"

#ifdef HAVE_DUCKDB_VALUE_BLOB

#include "./blob.h"

#endif /* HAVE_DUCKDB_VALUE_BLOB */

#ifdef HAVE_DUCKDB_APPENDER_CREATE

#include "./appender.h"

#endif /* HAVE_DUCKDB_APPENDER_CREATE */

extern VALUE mDuckDB;
extern VALUE cDuckDBDatabase;
extern VALUE cDuckDBConnection;

#ifdef HAVE_DUCKDB_VALUE_BLOB

extern VALUE cDuckDBBlob;

#endif /* HAVE_DUCKDB_VALUE_BLOB */

extern VALUE eDuckDBError;

#endif
