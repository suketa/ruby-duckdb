require 'test_helper'

if defined?(DuckDB::Config)
  module DuckDBTest
    class ConfigTest < Minitest::Test
      def test_s_new
        config = DuckDB::Config.new
        assert_instance_of(DuckDB::Config, config)
      end

      def test_s_size
        assert_operator(0, :<=, DuckDB::Config.size)
      end

      def test_s_get_config_flag
        key, value = DuckDB::Config.get_config_flag(0)
        assert_equal('access_mode', key)
        assert_match(/\AAccess mode of the database/, value)

        assert_raises(TypeError) do
          DuckDB::Config.get_config_flag('foo')
        end

        assert_raises(DuckDB::Error) do
          DuckDB::Config.get_config_flag(DuckDB::Config.size)
        end
      end

      def test_s_key_description
        key, value = DuckDB::Config.key_description(0)
        assert_equal('access_mode', key)
        assert_match(/\AAccess mode of the database/, value)
      end

      def test_s_key_descriptions
        h = DuckDB::Config.key_descriptions
        assert_instance_of(Hash, h)
        assert_match(/\AAccess mode of the database/, h['access_mode'])
      end

      def test_set_config
        config = DuckDB::Config.new
        config.set_config('access_mode', 'READ_ONLY')

        assert_raises(DuckDB::Error) do
          config.set_config('access_mode', 'INVALID_VALUE')
        end

        assert_raises(DuckDB::Error) do
          config.set_config('invalid-key', 'READ_ONLY')
        end
      end
    end
  end
end
