package bqsql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

func TestParamsAreBound(t *testing.T) {
	t.Parallel()

	evil := "x'); DROP TABLE people; --"
	p := bqsql.Param{Name: "s", Field: bqsql.Field{Type: "STRING"}, Value: evil}

	res, err := runSQL(t, newCatalog(t), "SELECT @s AS v, @s AS w", p)
	require.NoError(t, err)
	assert.Equal(t, [][]any{{evil, evil}}, res.Rows)

	res, err = runSQL(t, newCatalog(t), "UPDATE people SET name = @s WHERE id = 1", p)
	require.NoError(t, err)
	assert.Equal(t, int64(1), res.Affected)
	assert.Equal(t, evil, res.Effects[0].Rows[0][1])
}

func TestUpdatePathRejectsQuotes(t *testing.T) {
	t.Parallel()

	_, err := runSQL(t, newCatalog(t), "UPDATE people SET addr.`a\"b` = 'x' WHERE id = 1")
	require.Error(t, err)
	assert.Equal(t, bqsql.ErrInvalid, bqsql.AsError(err).Kind)
}

func TestNumericArgumentRanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		msg  string
	}{
		{"date year", "SELECT DATE(3000000000, 1, 1)", "invalid DATE arguments"},
		{"time hour", "SELECT TIME(3000000000, 1, 1)", "invalid TIME arguments"},
		{"datetime", "SELECT DATETIME(2020, 1, 1, 0, 0, 3000000000)", "out of range"},
		{"date add", "SELECT DATE_ADD(DATE '2020-01-01', INTERVAL 3000000000 DAY)", "out of range"},
		{"repeat", "SELECT REPEAT('a', 3000000000)", "out of range"},
		{"left", "SELECT LEFT('abc', 3000000000)", "invalid length argument"},
		{"regexp position", "SELECT REGEXP_EXTRACT('abc', 'b', 3000000000)", "out of range"},
		{"round digits", "SELECT ROUND(1.5, 3000000000)", "out of range"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := runSQL(t, newCatalog(t), tt.sql)
			require.Error(t, err)
			assert.Contains(t, bqsql.AsError(err).Message, tt.msg)
		})
	}
}

func TestLiteralsAndEscapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want any
		name string
		sql  string
	}{
		{"1500.0", "float", "SELECT 1.5e3"},
		{"2.0", "float whole", "SELECT 2.0"},
		{"QQ==", "bytes escape", `SELECT b'\x41'`},
		{"\u00e9", "unicode escape", `SELECT '\u00e9'`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := runSQL(t, newCatalog(t), tt.sql)
			require.NoError(t, err)
			require.Len(t, res.Rows, 1)
			assert.Equal(t, tt.want, res.Rows[0][0])
		})
	}
}
