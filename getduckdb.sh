#!/bin/sh

MACHINE=`uname -m`

case "$MACHINE" in
  "x86_64")
    ARCH=amd64
    ;;
  "aarch64")
    ARCH=arm64
    ;;
esac

# DuckDB 2.0 ships duckdb-shared-libs-*.tar.gz instead of libduckdb-*.zip.
wget -O duckdb.tar.gz "https://github.com/duckdb/duckdb/releases/download/v$DUCKDB_VERSION/duckdb-shared-libs-linux-$ARCH.tar.gz"
