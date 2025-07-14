#!/bin/sh

MACHINE=`uname -m`

case "$MACHINE" in
  "x86_64")
    ARCH=amd64
    ;;
  "aarch64")
    if printf '%s\n' '1.3.0' "$DUCKDB_VERSION" | sort -C -V; then
      ARCH=arm64
    else
      ARCH=aarch64
    fi
    ;;
esac

wget -O duckdb.zip "https://github.com/duckdb/duckdb/releases/download/v$DUCKDB_VERSION/libduckdb-linux-$ARCH.zip"
