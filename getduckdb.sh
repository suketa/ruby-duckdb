#!/bin/sh

MACHINE=`uname -m`

case "$MACHINE" in
  "x86_64" ) ARC=amd64 ;;
  "aarch64" ) ARC=aarch64 ;;
esac

wget -O duckdb.zip "https://github.com/duckdb/duckdb/releases/download/v$DUCKDB_VERSION/libduckdb-linux-$ARC.zip"
