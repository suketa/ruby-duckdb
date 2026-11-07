#include "ruby-duckdb.h"

static void set_row_valid(duckdb_vector vector, idx_t index) {
    uint64_t *validity = duckdb_vector_get_validity(vector);
    if (validity != NULL) {
        duckdb_validity_set_row_valid(validity, index);
    }
}

/*
 * *out = value * factor + sub (0 <= sub < factor), or 0 when the result does
 * not fit in int64_t. Near INT64_MIN the product alone can overflow while the
 * sum still fits, so that case is computed from value + 1.
 */
static int scale_add(int64_t value, int64_t factor, int64_t sub, int64_t *out) {
    int64_t high;

    if (value > (INT64_MAX - sub) / factor) {
        return 0;
    }
    if (value >= INT64_MIN / factor) {
        *out = value * factor + sub;
        return 1;
    }
    if (value < INT64_MIN / factor - 1) {
        return 0;
    }
    high = (value + 1) * factor;
    if (high - INT64_MIN < factor - sub) {
        return 0;
    }
    *out = high - (factor - sub);
    return 1;
}

void rbduckdb_vector_set_value_at(duckdb_vector vector, duckdb_logical_type element_type, idx_t index, VALUE value) {
    duckdb_type type_id;
    void* vector_data;
    uint64_t *validity;

    /* Handle NULL values */
    if (value == Qnil) {
        duckdb_vector_ensure_validity_writable(vector);
        validity = duckdb_vector_get_validity(vector);
        duckdb_validity_set_row_invalid(validity, index);
        return;
    }

    type_id = duckdb_get_type_id(element_type);
    vector_data = duckdb_vector_get_data(vector);

    switch(type_id) {
        case DUCKDB_TYPE_BOOLEAN:
            ((bool *)vector_data)[index] = RTEST(value);
            break;
        case DUCKDB_TYPE_TINYINT:
            ((int8_t *)vector_data)[index] = (int8_t)NUM2INT(value);
            break;
        case DUCKDB_TYPE_UTINYINT:
            ((uint8_t *)vector_data)[index] = (uint8_t)NUM2UINT(value);
            break;
        case DUCKDB_TYPE_SMALLINT:
            ((int16_t *)vector_data)[index] = (int16_t)NUM2INT(value);
            break;
        case DUCKDB_TYPE_USMALLINT:
            ((uint16_t *)vector_data)[index] = (uint16_t)NUM2UINT(value);
            break;
        case DUCKDB_TYPE_INTEGER:
            ((int32_t *)vector_data)[index] = NUM2INT(value);
            break;
        case DUCKDB_TYPE_UINTEGER:
            ((uint32_t *)vector_data)[index] = (uint32_t)NUM2ULL(value);
            break;
        case DUCKDB_TYPE_BIGINT:
            ((int64_t *)vector_data)[index] = NUM2LL(value);
            break;
        case DUCKDB_TYPE_UBIGINT:
            ((uint64_t *)vector_data)[index] = NUM2ULL(value);
            break;
        case DUCKDB_TYPE_HUGEINT: {
            duckdb_hugeint hugeint;
            hugeint.lower = NUM2ULL(rb_funcall(mDuckDBConverter, rb_intern("_hugeint_lower"), 1, value));
            hugeint.upper = NUM2LL(rb_funcall(mDuckDBConverter, rb_intern("_hugeint_upper"), 1, value));
            ((duckdb_hugeint *)vector_data)[index] = hugeint;
            break;
        }
        case DUCKDB_TYPE_UHUGEINT: {
            duckdb_uhugeint uhugeint;
            uhugeint.lower = NUM2ULL(rb_funcall(mDuckDBConverter, rb_intern("_hugeint_lower"), 1, value));
            uhugeint.upper = NUM2ULL(rb_funcall(mDuckDBConverter, rb_intern("_hugeint_upper"), 1, value));
            ((duckdb_uhugeint *)vector_data)[index] = uhugeint;
            break;
        }
        case DUCKDB_TYPE_FLOAT:
            ((float *)vector_data)[index] = (float)NUM2DBL(value);
            break;
        case DUCKDB_TYPE_DOUBLE:
            ((double *)vector_data)[index] = NUM2DBL(value);
            break;
        case DUCKDB_TYPE_VARCHAR: {
            VALUE str = rb_obj_as_string(value);
            const char *str_ptr = StringValuePtr(str);
            idx_t str_len = RSTRING_LEN(str);
            /*
             * Mark the row valid first: DuckDB sets it to NULL itself when
             * the string is not valid UTF-8, and that must not be undone.
             */
            set_row_valid(vector, index);
            duckdb_vector_assign_string_element_len(vector, index, str_ptr, str_len);
            return;
        }
        case DUCKDB_TYPE_BLOB: {
            VALUE str = rb_obj_as_string(value);
            const char *str_ptr = StringValuePtr(str);
            idx_t str_len = RSTRING_LEN(str);
            /*
             * Mark the row valid first: DuckDB sets it to NULL itself when
             * the string is not valid UTF-8, and that must not be undone.
             */
            set_row_valid(vector, index);
            duckdb_vector_assign_string_element_len(vector, index, str_ptr, str_len);
            return;
        }
        case DUCKDB_TYPE_TIMESTAMP: {
            duckdb_timestamp ts = rbduckdb_to_duckdb_timestamp_from_time_value(value);
            ((duckdb_timestamp *)vector_data)[index] = ts;
            break;
        }
        case DUCKDB_TYPE_TIMESTAMP_S: {
            duckdb_timestamp ts = rbduckdb_to_duckdb_timestamp_from_time_value(value);
            duckdb_timestamp_s ts_s;
            ts_s.seconds = ts.micros / 1000000;
            ((duckdb_timestamp_s *)vector_data)[index] = ts_s;
            break;
        }
        case DUCKDB_TYPE_TIMESTAMP_MS: {
            duckdb_timestamp ts = rbduckdb_to_duckdb_timestamp_from_time_value(value);
            duckdb_timestamp_ms ts_ms;
            ts_ms.millis = ts.micros / 1000;
            ((duckdb_timestamp_ms *)vector_data)[index] = ts_ms;
            break;
        }
        case DUCKDB_TYPE_TIMESTAMP_NS: {
            duckdb_timestamp ts = rbduckdb_to_duckdb_timestamp_from_time_value(value);
            duckdb_timestamp_ns ts_ns;
            int64_t sub_micro = NUM2LL(rb_funcall(value, rb_intern("nsec"), 0)) % 1000;
            if (!scale_add(ts.micros, 1000, sub_micro, &ts_ns.nanos)) {
                rb_raise(rb_eRangeError, "timestamp out of range for TIMESTAMP_NS");
            }
            ((duckdb_timestamp_ns *)vector_data)[index] = ts_ns;
            break;
        }
        case DUCKDB_TYPE_TIMESTAMP_TZ: {
            duckdb_timestamp ts;
            int64_t secs;
            if (!rb_obj_is_kind_of(value, rb_cTime)) {
                rb_raise(rb_eTypeError, "Expected Time object for TIMESTAMP_TZ");
            }
            /* An instant: microseconds since the Unix epoch, honoring utc_offset. */
            secs = NUM2LL(rb_funcall(value, rb_intern("to_i"), 0));
            if (!scale_add(secs, 1000000, NUM2LL(rb_funcall(value, rb_intern("usec"), 0)), &ts.micros)) {
                rb_raise(rb_eRangeError, "timestamp out of range for TIMESTAMP_TZ");
            }
            ((duckdb_timestamp *)vector_data)[index] = ts;
            break;
        }
        case DUCKDB_TYPE_DATE: {
            if (!rb_respond_to(value, rb_intern("year")) ||
                !rb_respond_to(value, rb_intern("month")) ||
                !rb_respond_to(value, rb_intern("day"))) {
                rb_raise(rb_eTypeError, "Expected Date object for DATE");
            }

            VALUE year = rb_funcall(value, rb_intern("year"), 0);
            VALUE month = rb_funcall(value, rb_intern("month"), 0);
            VALUE day = rb_funcall(value, rb_intern("day"), 0);

            duckdb_date date = rbduckdb_to_duckdb_date_from_value(year, month, day);
            ((duckdb_date *)vector_data)[index] = date;
            break;
        }
        case DUCKDB_TYPE_TIME: {
            if (!rb_obj_is_kind_of(value, rb_cTime)) {
                rb_raise(rb_eTypeError, "Expected Time object for TIME");
            }

            VALUE hour = rb_funcall(value, rb_intern("hour"), 0);
            VALUE min = rb_funcall(value, rb_intern("min"), 0);
            VALUE sec = rb_funcall(value, rb_intern("sec"), 0);
            VALUE usec = rb_funcall(value, rb_intern("usec"), 0);

            duckdb_time time = rbduckdb_to_duckdb_time_from_value(hour, min, sec, usec);
            ((duckdb_time *)vector_data)[index] = time;
            break;
        }
        case DUCKDB_TYPE_TIME_NS: {
            if (!rb_obj_is_kind_of(value, rb_cTime)) {
                rb_raise(rb_eTypeError, "Expected Time object for TIME_NS");
            }

            VALUE hour = rb_funcall(value, rb_intern("hour"), 0);
            VALUE min = rb_funcall(value, rb_intern("min"), 0);
            VALUE sec = rb_funcall(value, rb_intern("sec"), 0);
            VALUE nsec = rb_funcall(value, rb_intern("nsec"), 0);

            duckdb_time_ns time_ns = rbduckdb_to_duckdb_time_ns_from_value(hour, min, sec, nsec);
            ((duckdb_time_ns *)vector_data)[index] = time_ns;
            break;
        }
        case DUCKDB_TYPE_TIME_TZ: {
            if (!rb_obj_is_kind_of(value, rb_cTime)) {
                rb_raise(rb_eTypeError, "Expected Time object for TIME_TZ");
            }

            VALUE hour = rb_funcall(value, rb_intern("hour"), 0);
            VALUE min = rb_funcall(value, rb_intern("min"), 0);
            VALUE sec = rb_funcall(value, rb_intern("sec"), 0);
            VALUE usec = rb_funcall(value, rb_intern("usec"), 0);
            VALUE utc_offset = rb_funcall(value, rb_intern("utc_offset"), 0);

            duckdb_time t = rbduckdb_to_duckdb_time_from_value(hour, min, sec, usec);
            int64_t micros = t.micros;
            int32_t offset = NUM2INT(utc_offset);

            duckdb_time_tz time_tz = duckdb_create_time_tz(micros, offset);
            ((duckdb_time_tz *)vector_data)[index] = time_tz;
            break;
        }
        case DUCKDB_TYPE_INTERVAL: {
            VALUE months = rb_funcall(value, rb_intern("interval_months"), 0);
            VALUE days   = rb_funcall(value, rb_intern("interval_days"), 0);
            VALUE micros = rb_funcall(value, rb_intern("interval_micros"), 0);

            duckdb_interval interval;
            rbduckdb_to_duckdb_interval_from_value(&interval, months, days, micros);
            ((duckdb_interval *)vector_data)[index] = interval;
            break;
        }
        case DUCKDB_TYPE_UUID: {
            duckdb_hugeint hugeint;
            rbduckdb_uuid_str_to_hugeint(value, &hugeint);
            ((duckdb_hugeint *)vector_data)[index] = hugeint;
            break;
        }
        case DUCKDB_TYPE_DECIMAL: {
            uint8_t scale = duckdb_decimal_scale(element_type);
            duckdb_type internal_type = duckdb_decimal_internal_type(element_type);
            VALUE int_val = rb_funcall(mDuckDBConverter, id__decimal_to_unscaled, 2, value, INT2NUM(scale));

            switch (internal_type) {
                case DUCKDB_TYPE_SMALLINT:
                    ((int16_t *)vector_data)[index] = (int16_t)NUM2INT(int_val);
                    break;
                case DUCKDB_TYPE_INTEGER:
                    ((int32_t *)vector_data)[index] = NUM2INT(int_val);
                    break;
                case DUCKDB_TYPE_BIGINT:
                    ((int64_t *)vector_data)[index] = NUM2LL(int_val);
                    break;
                case DUCKDB_TYPE_HUGEINT: {
                    duckdb_hugeint hugeint;
                    hugeint.lower = NUM2ULL(rb_funcall(mDuckDBConverter, rb_intern("_hugeint_lower"), 1, int_val));
                    hugeint.upper = NUM2LL(rb_funcall(mDuckDBConverter, rb_intern("_hugeint_upper"), 1, int_val));
                    ((duckdb_hugeint *)vector_data)[index] = hugeint;
                    break;
                }
                default:
                    rb_raise(rb_eArgError, "Unsupported internal type for DECIMAL");
                    break;
            }
            break;
        }
        default:
            rb_raise(rb_eArgError, "Unsupported type for writing a value: DuckDB type id %d", (int)type_id);
            break;
    }

    /*
     * A cell may have been set to NULL earlier (e.g. a reused DataChunk), so
     * mark it valid again once the value has been written successfully.
     */
    set_row_valid(vector, index);
}

void rbduckdb_input_columns_load(duckdb_data_chunk input, idx_t n, duckdb_vector **vectors, duckdb_logical_type **types) {
    idx_t j;

    *vectors = ALLOC_N(duckdb_vector, n);
    *types = ALLOC_N(duckdb_logical_type, n);

    for (j = 0; j < n; j++) {
        (*vectors)[j] = duckdb_data_chunk_get_vector(input, j);
        (*types)[j] = duckdb_vector_get_column_type((*vectors)[j]);
    }
}

void rbduckdb_input_columns_free(duckdb_vector *vectors, duckdb_logical_type *types, idx_t n) {
    idx_t j;

    if (types != NULL) {
        for (j = 0; j < n; j++) {
            duckdb_destroy_logical_type(&types[j]);
        }
        xfree(types);
    }
    if (vectors != NULL) {
        xfree(vectors);
    }
}
