package timestreamquery

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRewriteTimestream(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	nowLit := "'2026-10-31 12:00:00.000000000'"

	tests := []struct {
		name string
		in   string
		want string
	}{
		{"measure column", "SELECT measure_value::double FROM t", `SELECT "measure_value::double" FROM t`},
		{"ago literal", "time > ago(15m)", "time > ts_ago(" + nowLit + ", 900000000000)"},
		{"ago interval", "time > ago(INTERVAL '2' DAY)", "time > ts_ago(" + nowLit + ", 172800000000000)"},
		{"bin literal", "bin(time, 1d)", "ts_bin(time, 86400000000000)"},
		{"bin long unit", "bin(time, 10day)", "ts_bin(time, 864000000000000)"},
		{"bin interval", "bin(time, INTERVAL '5' MINUTE)", "ts_bin(time, 300000000000)"},
		{"now", "now() > x", nowLit + " > x"},
		{"current timestamp", "current_timestamp", nowLit},
		{"signed duration", "time - 2h", "time - INTERVAL '7200' second"},
		{"string untouched", "x = 'ago(1h) 5m'", "x = 'ago(1h) 5m'"},
		{"ident untouched", `"measure_value::double" = 1`, `"measure_value::double" = 1`},
		{"identifier digits", "host1d = 1", "host1d = 1"},
		{"escaped quote", "x = 'it''s now()' AND now()", "x = 'it''s now()' AND " + nowLit},
		{"variable", "t < @scheduled_runtime", "t < 'V'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, rewriteTimestream(tt.in, now, map[string]string{"@scheduled_runtime": "V"}))
		})
	}
}

func TestJavaDouble(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want string
		in   float64
	}{
		{"0.0", 0}, {"100.0", 100}, {"60.25", 60.25}, {"-3.5", -3.5}, {"1.0E7", 1e7},
		{"1.0E-4", 1e-4}, {"1.2345E10", 12345000000}, {"0.001", 0.001},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, javaDouble(tt.in))
		})
	}
}

func TestDeriveSchema(t *testing.T) {
	t.Parallel()

	at := time.Unix(0, 0)
	recs := []SourceRecord{
		{Time: at, MeasureName: "cpu", MeasureValueType: "DOUBLE", MeasureValue: "1", Dimensions: []SourceDimension{{Name: "host", Value: "a"}}},
		{Time: at, MeasureName: "n", MeasureValueType: "BIGINT", MeasureValue: "1", Dimensions: []SourceDimension{{Name: "az", Value: "z"}}},
		{
			Time: at, MeasureName: "m", MeasureValueType: "MULTI",
			Measures: []SourceMeasure{{Name: "temp", Value: "1.5", Type: "DOUBLE"}, {Name: "up", Value: "true", Type: "BOOLEAN"}},
		},
	}

	var got []string
	for _, c := range deriveSchema(recs).cols {
		got = append(got, c.name+":"+c.typ+":"+c.attr)
	}

	assert.Equal(t, []string{
		"host:varchar:DIMENSION", "az:varchar:DIMENSION", "measure_name:varchar:MEASURE_NAME", "time:timestamp:TIMESTAMP",
		"measure_value::bigint:bigint:MEASURE_VALUE", "measure_value::double:double:MEASURE_VALUE",
		"temp:double:MULTI", "up:boolean:MULTI",
	}, got)
}

func TestUDFBin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   any
		step int64
		want any
	}{
		{"timestamp", "2022-06-17 10:15:20.000000000", 5 * 60e9, "2022-06-17 10:15:00.000000000"},
		{"day", "2022-06-17 10:15:20.000000000", 24 * 3600e9, "2022-06-17 00:00:00.000000000"},
		{"30 day", "2022-06-17 10:15:20.000000000", 30 * 24 * 3600e9, "2022-05-28 00:00:00.000000000"},
		{"integer", int64(17), 5, int64(15)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := udfBin(nil, []driver.Value{tt.in, tt.step})
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
