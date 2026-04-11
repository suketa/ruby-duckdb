# frozen_string_literal: true

module DuckDB
  # The Database class encapsulates a DuckDB database.
  #
  # The usage is as follows:
  #
  #   require 'duckdb'
  #
  #   db = DuckDB::Database.open # database in memory
  #   con = db.connect
  #
  #   con.query('CREATE TABLE users (id INTEGER, name VARCHAR(30))')
  #
  #   con.query("INSERT into users VALUES(1, 'Alice')")
  #   con.query("INSERT into users VALUES(2, 'Bob')")
  #   con.query("INSERT into users VALUES(3, 'Cathy')")
  #
  #   result = con.query('SELECT * from users')
  #   result.each do |row|
  #     p row
  #   end
  class Database
    # Opens a DuckDB database.
    #
    #   DuckDB::Database.new                    #=> in-memory database
    #   DuckDB::Database.new(:memory)           #=> in-memory database
    #   DuckDB::Database.new('test.db')         #=> file database
    #   DuckDB::Database.new('test.db', config: config) #=> with config
    #   DuckDB::Database.new(config: config)    #=> in-memory with config
    def initialize(path = :memory, config: nil, &block)
      if path.is_a?(Symbol) && path != :memory
        raise ArgumentError, "path must be a String or :memory, got #{path.inspect}"
      end

      dbpath = path == :memory ? nil : path
      _initialize(dbpath, config)
      _yield_self_and_close(&block) if block
    end

    class << self
      # Opens database.
      #
      #   DuckDB::Database.open                          #=> in-memory database
      #   DuckDB::Database.open('test.db')               #=> file database
      #   DuckDB::Database.open('test.db', config: config)
      #   DuckDB::Database.open(config: config)          #=> in-memory with config
      #
      #   DuckDB::Database.open do |db|
      #     con = db.connect
      #     con.query('CREATE TABLE users (id INTEGER, name VARCHAR(30))')
      #   end
      def open(path = :memory, *args, config: nil)
        path, config = _handle_deprecated_open_args(path, args, config)

        db = new(path, config: config)
        return db unless block_given?

        begin
          yield db
        ensure
          db.close
        end
      end

      private

      def _handle_deprecated_open_args(path, args, config)
        unless args.empty?
          raise TypeError, "expected DuckDB::Config, got #{args.first.class}" unless args.first.is_a?(DuckDB::Config)

          warn 'DuckDB::Database.open(path, config) is deprecated. ' \
               'Use DuckDB::Database.open(path, config: config) instead.'
          config = args.first
        end

        path = :memory if path.nil?
        [path, config]
      end
    end

    private

    def _yield_self_and_close
      yield self
    ensure
      close
    end

    public

    # connects database.
    #
    # The method yields block and disconnects the database if block given
    #
    #   db = DuckDB::Database.open
    #
    #   con = db.connect # => DuckDB::Connection
    #
    #   db.connect do |con|
    #     con.query('CREATE TABLE users (id INTEGER, name VARCHAR(30))')
    #   end
    def connect
      conn = _connect
      return conn unless block_given?

      begin
        yield conn
      ensure
        conn.disconnect
      end
    end
  end
end
