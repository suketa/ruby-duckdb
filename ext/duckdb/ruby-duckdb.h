#ifndef RUBY_DUCKDB_H
#define RUBY_DUCKDB_H

// #define DUCKDB_API_NO_DEPRECATED
#define DUCKDB_NO_EXTENSION_FUNCTIONS // disable extension C-functions

#include "ruby.h"
#include "ruby/thread.h"
#include <duckdb.h>

#ifdef HAVE_CONST_DUCKDB_TYPE_SQLNULL
#define HAVE_DUCKDB_H_GE_V1_1_1 1
#endif

#ifdef HAVE_DUCKDB_CREATE_INSTANCE_CACHE
#define HAVE_DUCKDB_H_GE_V1_2_0 1
#endif

#ifdef HAVE_DUCKDB_GET_TABLE_NAMES
#define HAVE_DUCKDB_H_GE_V1_3_0 1
#endif

#include "./error.h"
#include "./database.h"
#include "./connection.h"
#include "./result.h"
#include "./column.h"
#include "./logical_type.h"
#include "./prepared_statement.h"
#include "./extracted_statements.h"
#include "./pending_result.h"
#include "./util.h"
#include "./converter.h"

#include "./blob.h"
#include "./appender.h"
#include "./config.h"

#ifdef HAVE_DUCKDB_H_GE_V1_2_0
#include "./instance_cache.h"
#endif

extern VALUE mDuckDB;
extern VALUE cDuckDBDatabase;
extern VALUE cDuckDBConnection;
extern VALUE cDuckDBBlob;
extern VALUE cDuckDBConfig;
extern VALUE eDuckDBError;
extern VALUE mDuckDBConverter;
extern VALUE cDuckDBPreparedStatement;
extern VALUE PositiveInfinity;
extern VALUE NegativeInfinity;

#ifdef HAVE_DUCKDB_H_GE_V1_2_0
extern VALUE cDuckDBInstanceCache;
#endif

#endif
