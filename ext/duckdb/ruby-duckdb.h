#ifndef RUBY_DUCKDB_H
#define RUBY_DUCKDB_H

#include "ruby.h"
#include <duckdb.h>

#ifdef HAVE_DUCKDB_EXTRACT_STATEMENTS
#define HAVE_DUCKDB_H_GE_V070 1
#endif

#ifdef HAVE_DUCKDB_STRING_IS_INLINED
#define HAVE_DUCKDB_H_GE_V080 1
#endif

#include "./error.h"
#include "./database.h"
#include "./connection.h"
#include "./result.h"
#include "./column.h"
#include "./prepared_statement.h"
#include "./util.h"
#include "./converter.h"

#include "./blob.h"
#include "./appender.h"
#include "./config.h"

extern VALUE mDuckDB;
extern VALUE cDuckDBDatabase;
extern VALUE cDuckDBConnection;
extern VALUE cDuckDBBlob;
extern VALUE cDuckDBConfig;
extern VALUE eDuckDBError;
extern VALUE mDuckDBConverter;

#endif
