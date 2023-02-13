FROM ruby:3.2.1

RUN apt update -qq && \
    apt install -y build-essential curl git wget

RUN wget -O duckdb.zip "https://github.com/duckdb/duckdb/releases/download/v0.6.1/libduckdb-linux-amd64.zip"
RUN unzip duckdb.zip -d libduckdb
RUN mv libduckdb/duckdb.* /usr/local/include
RUN mv libduckdb/libduckdb.so /usr/local/lib
RUN ldconfig /usr/local/lib

COPY . /root/ruby-duckdb
WORKDIR /root/ruby-duckdb
RUN git config --global --add safe.directory /root/ruby-duckdb
RUN bundle install
RUN rake build
