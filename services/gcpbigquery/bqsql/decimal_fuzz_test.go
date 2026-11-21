package bqsql_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

func FuzzParseDecimal(f *testing.F) {
	for _, s := range []string{"0", "-1.5", "1e9", "1e-40", ".5", "5.", "+7", "1E+3", "", "x", bigMax, bigMin, "1e99999"} {
		f.Add(s, true)
	}

	f.Fuzz(func(t *testing.T, s string, big bool) {
		k := bqsql.KindNumeric
		if big {
			k = bqsql.KindBigNumeric
		}

		d, err := bqsql.ParseDecimal(k, s)
		if err != nil {
			require.ErrorIs(t, err, bqsql.ErrDecimal)

			return
		}

		again, err := bqsql.ParseDecimal(k, d.String())
		require.NoError(t, err)
		assert.Zero(t, d.Cmp(again))
		assert.Equal(t, d.String(), again.String())

		back, err := bqsql.DecodeSQLText(d.SQLText(), k)
		require.NoError(t, err)
		assert.Equal(t, d.String(), back.String())
		assert.True(t, bqsql.IsSQLText(d.SQLText()))
	})
}

func FuzzDecodeSQLText(f *testing.F) {
	f.Add("#" + strings.Repeat("0", 64))
	f.Add("#" + strings.Repeat("f", 64))
	f.Add("#8" + strings.Repeat("0", 63))
	f.Add("#8" + strings.Repeat("F", 63))
	f.Add("")

	f.Fuzz(func(t *testing.T, s string) {
		d, err := bqsql.DecodeSQLText(s, bqsql.KindBigNumeric)
		if err != nil {
			return
		}

		assert.True(t, bqsql.IsSQLText(s))
		assert.Equal(t, s, d.SQLText(), "an accepted encoding is canonical")
	})
}

func TestDecimalSQLTextCaseAndBoundaries(t *testing.T) {
	t.Parallel()

	upper := strings.ToUpper(dec(t, bqsql.KindNumeric, "255").SQLText()[1:])
	_, err := bqsql.DecodeSQLText("#"+upper, bqsql.KindNumeric)
	require.ErrorIs(t, err, bqsql.ErrDecimal)
	assert.False(t, bqsql.IsSQLText("#"+upper))
	assert.False(t, bqsql.IsSQLText(strings.ToUpper(dec(t, bqsql.KindNumeric, "1").SQLText())))

	ulp := "0.00000000000000000000000000000000000001"
	ordered := []string{bigMin, "-" + ulp, "0", ulp, bigMax}

	prev := ""

	for i, s := range ordered {
		enc := dec(t, bqsql.KindBigNumeric, s).SQLText()
		assert.Len(t, enc, 65)

		if i > 0 {
			assert.Less(t, prev, enc, s)
		}

		prev = enc
	}

	assert.Equal(t, "#"+strings.Repeat("0", 64), dec(t, bqsql.KindBigNumeric, bigMin).SQLText())
	assert.Equal(t, "#"+strings.Repeat("f", 64), dec(t, bqsql.KindBigNumeric, bigMax).SQLText())
	assert.Equal(t, dec(t, bqsql.KindNumeric, "1.5").SQLText(), dec(t, bqsql.KindBigNumeric, "1.5").SQLText())
	assert.Equal(t, dec(t, bqsql.KindNumeric, "0").SQLText(), dec(t, bqsql.KindNumeric, "-0").SQLText())
	assert.Equal(t, "#8"+strings.Repeat("0", 63), dec(t, bqsql.KindNumeric, "0").SQLText())
}
