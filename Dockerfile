ARG RUBY_VERSION=4.0.1
FROM ruby:${RUBY_VERSION}

ARG DUCKDB_VERSION=2.0.0
ARG VALGRIND_VERSION=3.21.0

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends build-essential curl git wget libc6-dbg && \
    apt-get clean

COPY getduckdb.sh .
RUN ./getduckdb.sh

RUN mkdir libduckdb && tar xzf duckdb.tar.gz -C libduckdb && \
    mv libduckdb/duckdb.* /usr/local/include && \
    mv libduckdb/libduckdb.so /usr/local/lib && \
    ldconfig /usr/local/lib

COPY . /root/ruby-duckdb
WORKDIR /root/ruby-duckdb
RUN git config --global --add safe.directory /root/ruby-duckdb
RUN bundle install && rake build
