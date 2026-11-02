# frozen_string_literal: true

require 'test_helper'
require 'tmpdir'
require 'fileutils'

module DuckDBTest
  class ResultVariantTest < Minitest::Test
    ERROR_MESSAGE = 'VARIANT result conversion is not supported'

    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      skip 'DuckDB does not support VARIANT' unless supports_variant?

      @directory = Dir.mktmpdir('ruby-duckdb-variant-')
    end

    def teardown
      @con&.close
      @db&.close
      FileUtils.remove_entry(@directory) if @directory
    end

    def test_rejects_non_null_variant_instead_of_returning_nil
      assert_equal([DuckDB::Error, ERROR_MESSAGE], conversion_error('SELECT 42::VARIANT'))
    end

    def test_unsupported_variant_is_rescuable_as_a_standard_error
      observed = begin
        @con.query('SELECT 42::VARIANT').to_a
        :no_error
      rescue StandardError => e
        e.class
      end

      assert_equal(DuckDB::Error, observed)
    end

    def test_reports_variant_column_type_without_reading_the_value
      assert_equal(:variant, @con.query('SELECT 42::VARIANT').columns.first.type)
    end

    def test_preserves_sql_null_variant
      assert_equal([[nil]], @con.query('SELECT NULL::VARIANT').to_a)
    end

    def test_preserves_null_variant_in_nested_containers
      sql = "SELECT map(['value'], [NULL::VARIANT]), {'value': NULL::VARIANT}, [NULL::VARIANT]"

      assert_equal([[{ 'value' => nil }, { value: nil }, [nil]]], @con.query(sql).to_a)
    end

    def test_rejects_non_null_variant_in_a_map
      assert_equal([DuckDB::Error, ERROR_MESSAGE], conversion_error("SELECT map(['value'], [42::VARIANT])"))
    end

    def test_rejects_non_null_variant_in_a_struct
      assert_equal([DuckDB::Error, ERROR_MESSAGE], conversion_error("SELECT {'value': 42::VARIANT}"))
    end

    def test_rejects_non_null_variant_in_a_list
      assert_equal([DuckDB::Error, ERROR_MESSAGE], conversion_error('SELECT [42::VARIANT]'))
    end

    def test_rejects_non_null_variant_in_an_array
      assert_equal([DuckDB::Error, ERROR_MESSAGE], conversion_error('SELECT [42::VARIANT]::VARIANT[1]'))
    end

    def test_rejects_non_null_variant_in_a_union
      assert_equal([DuckDB::Error, ERROR_MESSAGE], conversion_error('SELECT union_value(value := 42::VARIANT)'))
    end

    def test_retains_result_metadata_after_a_conversion_error
      result = @con.query('SELECT 42::VARIANT AS payload')
      begin
        result.to_a
      rescue DuckDB::Error
        # The Result still owns its metadata after the fetched chunk is freed.
      end

      assert_equal(['payload', :variant], [result.columns.first.name, result.columns.first.type])
    end

    def test_reuses_connection_after_repeated_conversion_errors_and_gc
      errors = 30.times.map do
        observed = conversion_error("SELECT map(['value'], [42::VARIANT])")
        GC.start
        observed
      end

      assert_equal([[[DuckDB::Error, ERROR_MESSAGE]] * 30, [[7]]], [errors, @con.query('SELECT 7').to_a])
    end

    def test_closes_connections_after_repeated_conversion_errors
      errors = 20.times.map do
        connection = @db.connect
        begin
          connection.query('SELECT [42::VARIANT]').to_a
          :no_error
        rescue DuckDB::Error => e
          [e.class, e.message]
        ensure
          connection.close
        end
      end
      GC.start

      assert_equal([[[DuckDB::Error, ERROR_MESSAGE]] * 20, [[7]]], [errors, @con.query('SELECT 7').to_a])
    end

    def test_releases_a_later_chunk_when_a_variant_follows_a_null_prefix
      result = @con.query(<<~SQL)
        SELECT CASE WHEN i < 8000 THEN NULL::VARIANT ELSE i::VARIANT END
        FROM range(10000) AS source(i)
      SQL
      observed_rows = 0
      error = begin
        result.each { |_row| observed_rows += 1 }
        :no_error
      rescue DuckDB::Error => e
        [e.class, e.message]
      end

      assert_equal([8000, [DuckDB::Error, ERROR_MESSAGE], [[7]]],
                   [observed_rows, error, @con.query('SELECT 7').to_a])
    end

    def test_rejects_variant_from_a_prepared_result
      observed = @con.prepare('SELECT 42::VARIANT') do |statement|
        statement.execute.to_a
        :no_error
      rescue DuckDB::Error => e
        [e.class, e.message]
      end

      assert_equal([DuckDB::Error, ERROR_MESSAGE], observed)
    end

    def test_rejects_variant_from_a_pending_result_and_reuses_connection
      observed = @con.async_query('SELECT 42::VARIANT') do |pending|
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
        while pending.state == :not_ready
          raise 'pending result exceeded ten seconds' if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

          pending.execute_task
        end
        begin
          pending.execute_pending.to_a
          :no_error
        rescue DuckDB::Error => e
          [e.class, e.message]
        end
      end

      assert_equal([[DuckDB::Error, ERROR_MESSAGE], [[7]]], [observed, @con.query('SELECT 7').to_a])
    end

    def test_copy_stats_rejects_non_null_variant_metadata
      skip 'extra_info was added in DuckDB 2.0' unless ::DuckDBTest.duckdb_2_0_or_later?

      assert_equal([DuckDB::Error, ERROR_MESSAGE], conversion_error(copy_sql))
    end

    def test_copy_file_is_complete_even_when_result_conversion_fails
      skip 'extra_info was added in DuckDB 2.0' unless ::DuckDBTest.duckdb_2_0_or_later?

      conversion_error(copy_sql)

      assert_equal([[1]], @con.query("SELECT * FROM read_parquet('#{output_path}')").to_a)
    end

    private

    def supports_variant?
      @con.query('SELECT typeof(42::VARIANT)').to_a == [['VARIANT']]
    rescue DuckDB::Error
      false
    end

    def conversion_error(sql)
      @con.query(sql).to_a
      :no_error
    rescue DuckDB::Error => e
      [e.class, e.message]
    end

    def output_path
      File.join(@directory, 'result.parquet')
    end

    def copy_sql
      "COPY (SELECT 1 AS id) TO '#{output_path}' (FORMAT PARQUET, RETURN_STATS)"
    end
  end
end
