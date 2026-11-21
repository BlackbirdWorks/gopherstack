package bqsql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Elements and rules from
// https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#format_elements_date_time.
func TestCastFormat(t *testing.T) {
	t.Parallel()

	runFnCases(t, []fnCase{
		{"year elements", "SELECT CAST(DATE '2018-02-03' AS STRING FORMAT 'YYYY/YYY/YY/Y/RRRR/RR')",
			[][]any{{"2018/018/18/8/2018/18"}}},
		{"short year padded", "SELECT CAST(DATE '0076-01-01' AS STRING FORMAT 'YYYY')", [][]any{{"0076"}}},
		{"month names", "SELECT CAST(DATE '2018-01-02' AS STRING FORMAT 'MON MONTH Mon Month mon month')",
			[][]any{{"JAN JANUARY Jan January jan january"}}},
		{"day names", "SELECT CAST(DATE '2021-01-03' AS STRING FORMAT 'DAY/DY/Dy/D/DD/DDD')",
			[][]any{{"SUNDAY   /SUN/Sun/1/03/003"}}},
		{"time elements", "SELECT CAST(TIME '21:30:07.16' AS STRING FORMAT 'HH24 HH HH12 MI SS SSSSS FF2 FF3 FF1')",
			[][]any{{"21 09 09 30 07 77407 16 160 2"}}},
		{"meridian case", "SELECT CAST(TIME '21:30:00' AS STRING FORMAT 'AM/am/A.M./a.m./aM')",
			[][]any{{"PM/pm/P.M./p.m./pm"}}},
		{"literals", `SELECT CAST(DATE '2018-01-02' AS STRING FORMAT 'DD "a\\"b" MM')`, [][]any{{`02 a"b 01`}}},
		{"timestamp zone", "SELECT CAST(TIMESTAMP '2008-12-25 00:00:00+00' AS STRING FORMAT 'HH24:MI TZH TZM' " +
			"AT TIME ZONE 'Asia/Kolkata')", [][]any{{"05:30 +05 30"}}},
		{"timestamp utc", "SELECT CAST(TIMESTAMP '2008-12-25 00:00:00+00' AS STRING FORMAT 'TZH TZM')",
			[][]any{{"+00 00"}}},
		{"parse date", "SELECT CAST('2018-Jan-02' AS DATE FORMAT 'YYYY-MON-DD')", [][]any{{"2018-01-02"}}},
		{"parse month name", "SELECT CAST('DECEMBER 25, 2020' AS DATE FORMAT 'MONTH DD, YYYY')",
			[][]any{{"2020-12-25"}}},
		{
			"parse timestamp meridian",
			"SELECT CAST('2020.06.03 11:15:53 P.M.' AS TIMESTAMP FORMAT 'YYYY.MM.DD HH12:MI:SS P.M.')",
			[][]any{{"1.591226153E9"}},
		},
		{
			"parse timestamp offset",
			"SELECT CAST('2020.06.03 00:00:53+02' AS TIMESTAMP FORMAT 'YYYY.MM.DD HH24:MI:SSTZH')",
			[][]any{{"1.591135253E9"}},
		},
		{"parse zone clause", "SELECT CAST('2020-06-03 12:00' AS TIMESTAMP FORMAT 'YYYY-MM-DD HH24:MI' " +
			"AT TIME ZONE 'Asia/Kolkata')", [][]any{{"1.5911658E9"}}},
		{"parse fraction", "SELECT CAST('01:02:03.25' AS TIME FORMAT 'HH24:MI:SS.FF2')", [][]any{{"01:02:03.25"}}},
		{"parse second of day", "SELECT CAST('03723' AS TIME FORMAT 'SSSSS')", [][]any{{"01:02:03"}}},
		{"date ignores zone", "SELECT CAST('2018-06-15' AS DATE FORMAT 'YYYY-MM-DD' AT TIME ZONE 'Asia/Kolkata')",
			[][]any{{"2018-06-15"}}},
		{"safe cast bad", "SELECT SAFE_CAST('nope' AS DATE FORMAT 'YYYY-MM-DD')", [][]any{{nil}}},
		{"null in", "SELECT CAST(CAST(NULL AS DATE) AS STRING FORMAT 'YYYY')", [][]any{{nil}}},
	})
}

func TestCastFormatErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		msg  string
	}{
		{"hour for date", "SELECT CAST(DATE '2018-01-02' AS STRING FORMAT 'HH24')", "not valid for DATE"},
		{"year for time", "SELECT CAST(TIME '01:02:03' AS STRING FORMAT 'YYYY')", "not valid for TIME"},
		{"tab in model", "SELECT CAST(DATE '2018-01-02' AS STRING FORMAT 'YYYY\tMM')", "Invalid format element"},
		{"repeated element", "SELECT CAST('2018 2019' AS DATE FORMAT 'YYYY YYYY')", "more than once"},
		{"hh without meridian", "SELECT CAST('10' AS TIME FORMAT 'HH12')", "meridian"},
		{"ddd with month", "SELECT CAST('2018 034 01' AS DATE FORMAT 'YYYY DDD MM')", "DDD"},
		{"bad input", "SELECT CAST('2018-13-02' AS DATE FORMAT 'YYYY-MM-DD')", "Invalid"},
		{"trailing text", "SELECT CAST('2018-01-02x' AS DATE FORMAT 'YYYY-MM-DD')", "trailing"},
		{"day zero", "SELECT CAST('2018-01-00' AS DATE FORMAT 'YYYY-MM-DD')", "Invalid"},
		{"year zero", "SELECT CAST('0000-01-01' AS DATE FORMAT 'YYYY-MM-DD')", "Invalid"},
		{"offset hour range", "SELECT CAST('2018-01-01 +99' AS TIMESTAMP FORMAT 'YYYY-MM-DD TZH')", "Invalid"},
		{"offset minute range", "SELECT CAST('2018-01-01 +01:75' AS TIMESTAMP FORMAT 'YYYY-MM-DD TZH:TZM')", "Invalid"},
		{"numeric unsupported", "SELECT CAST(1 AS STRING FORMAT '9')", "STRING and DATE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := runSQL(t, newCatalog(t), tt.sql)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.msg)
		})
	}
}
