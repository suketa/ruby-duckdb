ARG RUBY_VERSION=3.4.4
FROM ruby:${RUBY_VERSION}

ARG DUCKDB_VERSION=1.3.0
ARG VALGRIND_VERSION=3.21.0

RUN apt update -qq && \
    apt install -y build-essential curl git wget libc6-dbg

COPY getduckdb.sh .
RUN ./getduckdb.sh

RUN unzip duckdb.zip -d libduckdb && \
    mv libduckdb/duckdb.* /usr/local/include && \
    mv libduckdb/libduckdb.so /usr/local/lib && \
    ldconfig /usr/local/lib

RUN mkdir valgrind-tmp && \
    cd valgrind-tmp && \
    wget https://sourceware.org/pub/valgrind/valgrind-${VALGRIND_VERSION}.tar.bz2 && \
    tar xf valgrind-${VALGRIND_VERSION}.tar.bz2 && \
    cd valgrind-${VALGRIND_VERSION} && \
    ./configure && \
    make -s && \
    make -s install && \
    cd .. && \
    rm -rf /valgrind-tmp

COPY . /root/ruby-duckdb
WORKDIR /root/ruby-duckdb
RUN git config --global --add safe.directory /root/ruby-duckdb
RUN bundle install && rake build
