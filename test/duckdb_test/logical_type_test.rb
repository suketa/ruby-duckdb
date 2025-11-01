# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class LogicalTypeTest < Minitest::Test
    CREATE_TYPE_ENUM_SQL = <<~SQL
      CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy', '𝘾𝝾օɭ 😎');
    SQL

    CREATE_TABLE_SQL = <<~SQL
      CREATE TABLE table1(
        boolean_col BOOLEAN,
        tinyint_col TINYINT,
        smallint_col SMALLINT,
        integer_col INTEGER,
        bigint_col BIGINT,
        utinyint_col UTINYINT,
        usmallint_col USMALLINT,
        uinteger_col UINTEGER,
        ubigint_col UBIGINT,
        real_col REAL,
        double_col DOUBLE,
        date_col DATE,
        time_col TIME,
        timestamp_col timestamp,
        interval_col INTERVAL,
        hugeint_col HUGEINT,
        varchar_col VARCHAR,
        decimal_col DECIMAL(9, 6),
        enum_col mood,
        int_list_col INT[],
        varchar_list_col VARCHAR[],
        int_array_col INT[3],
        varchar_list_array_col VARCHAR[2],
        struct_col STRUCT(word VARCHAR, length INTEGER),
        uuid_col UUID,
        map_col MAP(INTEGER, VARCHAR),
        union_col UNION(num INTEGER, str VARCHAR)
      );
    SQL

    INSERT_SQL = <<~SQL
      INSERT INTO table1 VALUES
      (
        true,
        1,
        32767,
        2147483647,
        9223372036854775807,
        1,
        32767,
        2147483647,
        9223372036854775807,
        12345.375,
        123.456789,
        '2019-11-03',
        '12:34:56',
        '2019-11-03 12:34:56',
        '1 day',
        170141183460469231731687303715884105727,
        'string',
        123.456789,
        'sad',
        [1, 2, 3],
        ['a', 'b', 'c'],
        [1, 2, 3],
        [['a', 'b'], ['c', 'd']],
        ROW('Ruby', 4),
        '#{SecureRandom.uuid}',
        MAP{1: 'foo'},
        1::INTEGER,
      )
    SQL

    SELECT_SQL = 'SELECT * FROM table1'

    EXPECTED_TYPES = %i[
      boolean
      tinyint
      smallint
      integer
      bigint
      utinyint
      usmallint
      uinteger
      ubigint
      float
      double
      date
      time
      timestamp
      interval
      hugeint
      varchar
      decimal
      enum
      list
      list
      array
      array
      struct
      uuid
      map
      union
    ].freeze

    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      create_data(@con)
      result = @con.query(SELECT_SQL)
      @columns = result.columns
    end

    def test_singleton_method_defined
      %i[
        boolean
        tinyint
        smallint
        integer
        bigint
        utinyint
        usmallint
        uinteger
        ubigint
        float
        double
        timestamp
        date
        time
        interval
        hugeint
        uhugeint
        varchar
        blob
        timestamp_s
        timestamp_ms
        timestamp_ns
        bit
        time_tz
        timestamp_tz
        bignum
        sqlnull
        string_literal
        integer_literal
      ].each do |method_name|
        logical_type = DuckDB::LogicalType.send(method_name)
        assert_instance_of(DuckDB::LogicalType, logical_type)
        assert_equal(method_name, logical_type.type)
      end
    end

    def test_type
      logical_types = @columns.map(&:logical_type)
      assert_equal(EXPECTED_TYPES, logical_types.map(&:type))
    end

    def test_alias
      enum_column = @columns.find { |column| column.type == :enum }
      enum_logical_type = enum_column.logical_type
      assert_equal("mood", enum_logical_type.alias=("mood"))
      assert_equal("mood", enum_logical_type.alias)
    end

    def test_decimal_internal_type
      decimal_column = @columns.find { |column| column.type == :decimal }
      assert_equal(:integer, decimal_column.logical_type.internal_type)
    end

    def test_decimal_width
      decimal_column = @columns.find { |column| column.type == :decimal }
      assert_equal(9, decimal_column.logical_type.width)
    end

    def test_decimal_scale
      decimal_column = @columns.find { |column| column.type == :decimal }
      assert_equal(6, decimal_column.logical_type.scale)
    end

    def test_list_child_type
      list_columns = @columns.select { |column| column.type == :list }
      child_types = list_columns.map do |list_column|
        list_column.logical_type.child_type
      end
      assert(child_types.all? { |child_type| child_type.is_a?(DuckDB::LogicalType) })
      assert_equal([:integer, :varchar], child_types.map(&:type))
    end

    def test_map_child_type
      map_column = @columns.detect { |column| column.type == :map }
      child_type = map_column.logical_type.child_type
      assert(child_type.is_a?(DuckDB::LogicalType))
      assert_equal(:struct, child_type.type)
    end

    def test_array_child_type
      array_columns = @columns.select { |column| column.type == :array }
      child_types = array_columns.map do |array_column|
        array_column.logical_type.child_type
      end
      assert(child_types.all? { |child_type| child_type.is_a?(DuckDB::LogicalType) })
      assert_equal([:integer, :varchar], child_types.map(&:type))
    end

    def test_array_size
      array_columns = @columns.select { |column| column.type == :array }
      array_sizes = array_columns.map do |array_column|
        array_column.logical_type.size
      end
      assert_equal([3, 2], array_sizes)
    end

    def test_map_key_type
      map_column = @columns.find { |column| column.type == :map }
      key_type = map_column.logical_type.key_type
      assert(key_type.is_a?(DuckDB::LogicalType))
      assert_equal(:integer, key_type.type)
    end

    def test_map_value_type
      map_column = @columns.find { |column| column.type == :map }
      value_type = map_column.logical_type.value_type
      assert(value_type.is_a?(DuckDB::LogicalType))
      assert_equal(:varchar, value_type.type)
    end

    def test_union_member_count
      union_column = @columns.find { |column| column.type == :union }
      assert_equal(2, union_column.logical_type.member_count)
    end

    def test_union_each_member_name
      union_column = @columns.find { |column| column.type == :union }
      union_logical_type = union_column.logical_type
      member_names = union_logical_type.each_member_name.to_a
      assert_equal(["num", "str"], member_names)
    end

    def test_union_each_member_type
      union_column = @columns.find { |column| column.type == :union }
      union_logical_type = union_column.logical_type
      member_types = union_logical_type.each_member_type.to_a
      assert(member_types.all? { |member_type| member_type.is_a?(DuckDB::LogicalType) })
      assert_equal([:integer, :varchar], member_types.map(&:type))
    end

    def test_struct_child_count
      struct_column = @columns.find { |column| column.type == :struct }
      assert_equal(2, struct_column.logical_type.child_count)
    end

    def test_struct_each_child_name
      struct_column = @columns.find { |column| column.type == :struct }
      struct_logical_type = struct_column.logical_type
      child_names = struct_logical_type.each_child_name.to_a
      assert_equal(["word", "length"], child_names)
    end

    def test_struct_each_child_type
      struct_column = @columns.find { |column| column.type == :struct }
      struct_logical_type = struct_column.logical_type
      child_types = struct_logical_type.each_child_type.to_a
      assert(child_types.all? { |child_type| child_type.is_a?(DuckDB::LogicalType) })
      assert_equal([:varchar, :integer], child_types.map(&:type))
    end

    def test_enum_internal_type
      enum_column = @columns.find { |column| column.type == :enum }
      assert_equal(:utinyint, enum_column.logical_type.internal_type)
    end

    def test_enum_dictionary_size
      enum_column = @columns.find { |column| column.type == :enum }
      assert_equal(4, enum_column.logical_type.dictionary_size)
    end

    def test_enum_each_dictionary_value
      enum_column = @columns.find { |column| column.type == :enum }
      enum_logical_type = enum_column.logical_type
      dictionary_values = enum_logical_type.each_dictionary_value.to_a
      assert_equal(["sad", "ok", "happy", "𝘾𝝾օɭ 😎"], dictionary_values)
    end

    # This test is for the new DuckDB::LogicalType.new method.
    def test_new
      # DUCKDB_TYPE_INTEGER = 4
      int_type = DuckDB::LogicalType.new(4)
      assert_instance_of(DuckDB::LogicalType, int_type)
      assert_equal(:integer, int_type.type)

      # DUCKDB_TYPE_VARCHAR = 17
      varchar_type = DuckDB::LogicalType.new(17)
      assert_instance_of(DuckDB::LogicalType, varchar_type)
      assert_equal(:varchar, varchar_type.type)
    end

    def test_new_with_invalid_type
      # Using a currently unassigned integer value
      assert_raises(ArgumentError) do
        DuckDB::LogicalType.new(999)
      end
    end

    def test_new_with_complex_types
      # DUCKDB_TYPE_DECIMAL = 19, requires width and scale
      assert_raises(ArgumentError) { DuckDB::LogicalType.new(19) }

      # DUCKDB_TYPE_LIST = 24, requires a child type
      assert_raises(ArgumentError) { DuckDB::LogicalType.new(24) }
    end

    def test_new_with_primitive_like_complex_type
      # DUCKDB_TYPE_BIT = 29
      bit_type = DuckDB::LogicalType.new(29)
      assert_instance_of(DuckDB::LogicalType, bit_type)
      assert_equal(:bit, bit_type.type)
    end

    private

    def create_data(con)
      con.query(CREATE_TYPE_ENUM_SQL)
      con.query(CREATE_TABLE_SQL)
      con.query(INSERT_SQL)
    end
  end
end
