# frozen_string_literal: true

require 'test_helper'
require 'fileutils'
require 'tmpdir'

if defined?(DuckDB::InstanceCache)

  module DuckDBTest
    class InstanceCacheTest < Minitest::Test
      def test_s_new
        assert_instance_of DuckDB::InstanceCache, DuckDB::InstanceCache.new
      end

      def test_get_or_create
        skip 'Thread.join hangs on Windows' if RUBY_PLATFORM.match?(/mingw|mswin|cygwin/)

        cache = DuckDB::InstanceCache.new
        path = 'test_shared_db.db'
        30.times do
          run_threaded_cache_test(cache, path)
          FileUtils.rm_f(path)
        end
      end

      def test_get_or_create_without_path
        cache = DuckDB::InstanceCache.new
        db = cache.get_or_create

        assert_instance_of DuckDB::Database, db
        db.close
      end

      def test_get_or_create_with_empty_path
        cache = DuckDB::InstanceCache.new
        db = cache.get_or_create('')

        assert_instance_of DuckDB::Database, db
        db.close
      end

      def test_get_or_create_with_memory
        cache = DuckDB::InstanceCache.new
        db = cache.get_or_create(':memory:')

        assert_instance_of DuckDB::Database, db
        db.close
      end

      def test_get_or_create_with_config
        cache = DuckDB::InstanceCache.new
        config = create_desc_config
        db = cache.get_or_create(nil, config)
        con = db.connect
        setup_test_table(con)

        result = con.query('SELECT number FROM numbers ORDER BY number')

        assert_equal(4, result.first.first)
        con.close
        db.close
        cache.destroy
      end

      def test_destroy
        cache = DuckDB::InstanceCache.new

        assert_nil cache.destroy
      end

      def test_get_or_create_returns_the_same_database_for_the_same_path
        with_cached_path do |cache, path|
          db = cache.get_or_create(path)

          assert_same db, cache.get_or_create(path)
          db.close
        end
      end

      # Registrations live in the shared instance's catalog, so a wrapper that
      # goes away must not take another wrapper's functions with it.
      def test_registered_function_outlives_a_dropped_wrapper_over_the_same_instance
        with_cached_path do |cache, path|
          db = cache.get_or_create(path)
          con = db.connect
          register_doubler(cache, path)
          3.times { GC.start }

          assert_equal [[42]], con.query('SELECT dbl(21)').to_a
          con.disconnect
          db.close
        end
      end

      # Each of these opens a fresh instance rather than sharing one, so they
      # must not be collapsed onto a single wrapper.
      def test_get_or_create_without_a_file_path_returns_distinct_databases
        cache = DuckDB::InstanceCache.new
        [nil, '', ':memory:'].each do |path|
          first = cache.get_or_create(path)
          second = cache.get_or_create(path)

          refute_same first, second, "expected distinct databases for #{path.inspect}"
          first.close
          second.close
        end
      end

      def test_separate_caches_return_distinct_databases_for_the_same_path
        skip 'Windows cannot open one database file from two instances' if RUBY_PLATFORM.match?(/mingw|mswin|cygwin/)

        with_cached_path do |cache, path|
          first = cache.get_or_create(path)
          second = DuckDB::InstanceCache.new.get_or_create(path)

          refute_same first, second
          first.close
          second.close
        end
      end

      def test_close_evicts_the_database_from_the_cache
        with_cached_path do |cache, path|
          closed = cache.get_or_create(path)
          closed.close

          reopened = cache.get_or_create(path)
          con = reopened.connect

          refute_same closed, reopened
          assert_equal [[1]], con.query('SELECT 1').to_a
          con.disconnect
          reopened.close
        end
      end

      private

      # Every database opened under the yielded path must be closed before the
      # block ends: Windows refuses to delete a file that is still open, so an
      # unclosed wrapper fails the tmpdir cleanup rather than the assertion.
      def with_cached_path
        Dir.mktmpdir do |dir|
          yield DuckDB::InstanceCache.new, File.join(dir, 'cached.duckdb')
        end
      end

      # Registers in a scope that is dropped, so the wrapper it used becomes
      # collectable while the caller's wrapper stays alive.
      def register_doubler(cache, path)
        con = cache.get_or_create(path).connect
        function = DuckDB::ScalarFunction.new
        function.name = 'dbl'
        function.add_parameter(DuckDB::LogicalType::INTEGER)
        function.return_type = DuckDB::LogicalType::INTEGER
        function.set_function { |value| value * 2 }
        con.register_scalar_function(function)
        con.disconnect
        nil
      end

      def run_threaded_cache_test(cache, path)
        thread = Thread.new do
          db = cache.get_or_create(path)

          assert_instance_of DuckDB::Database, db
          db.close
        end
        db = cache.get_or_create(path)

        assert_instance_of DuckDB::Database, db
        db.close
        thread.join
      end

      def create_desc_config
        config = DuckDB::Config.new
        config['default_order'] = 'DESC'
        config
      end

      def setup_test_table(con)
        con.query('CREATE TABLE numbers (number INTEGER)')
        con.query('INSERT INTO numbers VALUES (2), (1), (4), (3)')
      end
    end
  end

end
