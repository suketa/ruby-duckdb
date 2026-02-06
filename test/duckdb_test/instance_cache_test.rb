# frozen_string_literal: true

require 'test_helper'
require 'fileutils'

if defined?(DuckDB::InstanceCache)

  module DuckDBTest
    class InstanceCacheTest < Minitest::Test
      def test_s_new
        assert_instance_of DuckDB::InstanceCache, DuckDB::InstanceCache.new
      end

      def test_get_or_create
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

      private

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
