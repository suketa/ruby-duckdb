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
          thread = Thread.new do
            db = cache.get_or_create(path)

            assert_instance_of DuckDB::Database, db
            db.close
          end
          db = cache.get_or_create(path)

          assert_instance_of DuckDB::Database, db
          db.close
          thread.join

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
        config = DuckDB::Config.new
        config['default_order'] = 'DESC'
        db = cache.get_or_create(nil, config)
        con = db.connect
        con.query('CREATE TABLE numbers (number INTEGER)')
        con.query('INSERT INTO numbers VALUES (2), (1), (4), (3)')

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
    end
  end

end
