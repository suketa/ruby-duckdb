# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ConfigTest < Minitest::Test
    def test_s_new
      config = DuckDB::Config.new

      assert_instance_of(DuckDB::Config, config)
    end

    def test_s_size
      assert_operator(0, :<=, DuckDB::Config.size)
    end

    def test_s_get_config_flag_key_and_value_is_string
      key, value = DuckDB::Config.get_config_flag(0)

      assert_instance_of(String, key)
      assert_instance_of(String, value)
    end

    def test_s_get_config_flag_key_and_value_length_positive
      key, value = DuckDB::Config.get_config_flag(0)

      assert_operator(key.length, :>, 0)
      assert_operator(value.length, :>, 0)
    end

    def test_s_get_config_flag_with_invalid_arguments
      assert_raises(TypeError) do
        DuckDB::Config.get_config_flag('foo')
      end

      assert_raises(DuckDB::Error) do
        DuckDB::Config.get_config_flag(DuckDB::Config.size)
      end
    end

    def test_s_key_description_key_and_value_is_string
      key, value = DuckDB::Config.key_description(0)

      assert_instance_of(String, key)
      assert_instance_of(String, value)
    end

    def test_s_key_description_key_and_value_length_positive
      key, value = DuckDB::Config.key_description(0)

      assert_operator(key.length, :>, 0)
      assert_operator(value.length, :>, 0)
    end

    def test_s_key_descriptions
      h = DuckDB::Config.key_descriptions

      assert_instance_of(Hash, h)
      assert_match(/\AAccess mode of the database/, h['access_mode'])
    end

    def test_set_config
      config = DuckDB::Config.new

      assert_instance_of(DuckDB::Config, config.set_config('access_mode', 'READ_ONLY'))

      assert_raises(DuckDB::Error) do
        config.set_config('access_mode', 'INVALID_VALUE')
      end
    end

    def test_set_invalid_option
      config = DuckDB::Config.new

      assert_instance_of(DuckDB::Config, config.set_config('aaa_invalid_option', 'READ_ONLY'))
      assert_raises(DuckDB::Error) do
        DuckDB::Database.open(config: config)
      end
    end

    def test_duckdb_api_config_set_to_ruby_default
      db = DuckDB::Database.open
      conn = db.connect
      result = conn.query('PRAGMA user_agent')
      user_agent = result.first[0]

      assert_includes(user_agent.downcase, 'ruby', "User agent should contain 'ruby', but got: #{user_agent}")
      db.close
    end

    def test_duckdb_api_config_set_to_ruby_custom
      config = DuckDB::Config.new
      config.set_config('threads', '2')
      db = DuckDB::Database.open(config: config)
      conn = db.connect
      result = conn.query('PRAGMA user_agent')
      user_agent = result.first[0]

      assert_includes(user_agent.downcase, 'ruby',
                      "User agent should contain 'ruby' even with custom config, but got: #{user_agent}")
      db.close
    end
  end
end
