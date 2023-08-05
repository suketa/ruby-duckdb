ARG RUBY_VERSION=3.2.2
FROM ruby:${RUBY_VERSION}

ARG DUCKDB_VERSION=0.8.1

RUN apt update -qq && \
    apt install -y build-essential curl git wget

COPY getduckdb.sh .
RUN ./getduckdb.sh

RUN unzip duckdb.zip -d libduckdb
RUN mv libduckdb/duckdb.* /usr/local/include
RUN mv libduckdb/libduckdb.so /usr/local/lib
RUN ldconfig /usr/local/lib

COPY . /root/ruby-duckdb
WORKDIR /root/ruby-duckdb
RUN git config --global --add safe.directory /root/ruby-duckdb
RUN bundle install
RUN rake build
