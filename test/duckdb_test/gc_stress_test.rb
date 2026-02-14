# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  # Tests for GC stress and compaction safety
  # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
  class GCStressTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      @con.execute('SET threads=1')
    end

    def teardown
      @con&.close
      @db&.close
    end

    def test_multiple_scalar_functions_with_gc_compaction
      skip 'GC.compact not available' unless GC.respond_to?(:compact)

      # Register multiple scalar functions
      5.times do |i|
        multiplier = i + 1
        @con.register_scalar_function(DuckDB::ScalarFunction.new.tap do |sf|
          sf.name = "multiply_by_#{i + 1}"
          sf.add_parameter(DuckDB::LogicalType::INTEGER)
          sf.return_type = DuckDB::LogicalType::INTEGER
          sf.set_function { |v| v * multiplier }
        end)
      end

      # Compact multiple times and test all functions
      10.times do
        GC.compact

        # Test each function
        5.times do |i|
          result = @con.execute("SELECT multiply_by_#{i + 1}(10)")
          expected = 10 * (i + 1)

          assert_equal expected, result.first.first, "Function #{i + 1} failed after compaction"
        end
      end
    end

    def test_scalar_function_aggressive_gc_stress
      skip 'GC.compact not available' unless GC.respond_to?(:compact)

      # Capture multiple local variables
      base = 100
      offset = 5

      @con.register_scalar_function(DuckDB::ScalarFunction.new.tap do |sf|
        sf.name = 'complex_calc'
        sf.add_parameter(DuckDB::LogicalType::INTEGER)
        sf.return_type = DuckDB::LogicalType::INTEGER
        sf.set_function { |v| (v + offset) * base }
      end)

      # Aggressive GC with stress mode
      old_stress = GC.stress
      GC.stress = true

      begin
        20.times do |i|
          GC.start
          GC.compact if i.even?

          result = @con.execute('SELECT complex_calc(3)')

          assert_equal 800, result.first.first # (3 + 5) * 100
        end
      ensure
        GC.stress = old_stress
      end
    end

    # rubocop:disable Minitest/MultipleAssertions
    def test_table_function_with_gc_compaction
      skip 'GC.compact not available' unless GC.respond_to?(:compact)

      # Capture local variables
      multiplier = 3
      done = false

      tf = DuckDB::TableFunction.new
      tf.name = 'test_gc_table'

      tf.bind do |bind_info|
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      tf.init do |_init_info|
        done = false
      end

      tf.execute do |_func_info, output|
        if done
          output.size = 0
        else
          3.times do |i|
            output.set_value(0, i, i * multiplier)
          end
          output.size = 3
          done = true
        end
      end

      @con.register_table_function(tf)

      # Test with multiple compactions
      10.times do
        GC.compact

        result = @con.query('SELECT * FROM test_gc_table() ORDER BY value')
        rows = result.each.to_a

        assert_equal 3, rows.size
        assert_equal 0, rows[0][0]
        assert_equal 3, rows[1][0]
        assert_equal 6, rows[2][0]

        # Reset done for next iteration
        done = false
      end
    end
    # rubocop:enable Minitest/MultipleAssertions

    def test_mixed_functions_gc_stress
      skip 'GC.compact not available' unless GC.respond_to?(:compact)

      # Register both scalar and table functions
      @con.register_scalar_function(DuckDB::ScalarFunction.new.tap do |sf|
        sf.name = 'double_it'
        sf.add_parameter(DuckDB::LogicalType::BIGINT)
        sf.return_type = DuckDB::LogicalType::BIGINT
        sf.set_function { |v| v * 2 }
      end)

      done = false
      tf = DuckDB::TableFunction.new
      tf.name = 'simple_range'

      tf.bind do |bind_info|
        bind_info.add_result_column('n', DuckDB::LogicalType::BIGINT)
      end

      tf.init do |_init_info|
        done = false
      end

      tf.execute do |_func_info, output|
        if done
          output.size = 0
        else
          5.times do |i|
            output.set_value(0, i, i)
          end
          output.size = 5
          done = true
        end
      end

      @con.register_table_function(tf)

      # Use both functions together with GC stress
      10.times do
        GC.compact

        result = @con.query('SELECT double_it(n) as doubled FROM simple_range() ORDER BY n')
        rows = result.each.to_a

        assert_equal 5, rows.size
        assert_equal([0, 2, 4, 6, 8], rows.map { |r| r[0] })

        done = false
      end
    end
  end
  # rubocop:enable Metrics/AbcSize, Metrics/MethodLength
end
