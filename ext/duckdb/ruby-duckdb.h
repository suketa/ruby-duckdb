#ifndef RUBY_DUCKDB_H
#define RUBY_DUCKDB_H

#include "ruby.h"
#include <duckdb.h>

#ifdef HAVE_DUCKDB_APPENDER_COLUMN_COUNT
#define HAVE_DUCKDB_H_GE_V0_10_0 1
#endif

#ifdef HAVE_DUCKDB_FETCH_CHUNK
#define HAVE_DUCKDB_H_GE_V1_0_0 1
#endif

#include "./error.h"
#include "./database.h"
#include "./connection.h"
#include "./result.h"
#include "./column.h"
#include "./prepared_statement.h"
#include "./extracted_statements.h"
#include "./pending_result.h"
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
extern VALUE cDuckDBPreparedStatement;

#endif
