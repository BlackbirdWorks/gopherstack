package bqsql_test

import (
	"math/rand/v2"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

func randomDecimal(r *rand.Rand, k bqsql.Kind) string {
	digits := 1 + r.IntN(36)
	if k == bqsql.KindBigNumeric {
		digits = 1 + r.IntN(70)
	}

	var sb strings.Builder

	if r.IntN(2) == 0 {
		sb.WriteByte('-')
	}

	for range digits {
		sb.WriteByte(byte('0' + r.IntN(10)))
	}

	frac := r.IntN(9)
	s := sb.String()

	if k == bqsql.KindBigNumeric {
		frac = r.IntN(38)
	}

	if frac > 0 && len(s) > frac+1 {
		s = s[:len(s)-frac] + "." + s[len(s)-frac:]
	}

	return s
}

func TestDecimalSQLTextOrdering(t *testing.T) {
	t.Parallel()

	r := rand.New(rand.NewPCG(1, 2))

	var vals []bqsql.Decimal

	for _, s := range []string{"0", "-0.000000001", "0.000000001", numMax, "-" + numMax, "1", "-1"} {
		vals = append(vals, dec(t, bqsql.KindNumeric, s))
	}

	for _, s := range []string{"0", bigMax, bigMin, "0.00000000000000000000000000000000000001"} {
		vals = append(vals, dec(t, bqsql.KindBigNumeric, s))
	}

	for range 400 {
		k := bqsql.KindNumeric
		if r.IntN(2) == 0 {
			k = bqsql.KindBigNumeric
		}

		d, err := bqsql.ParseDecimal(k, randomDecimal(r, k))
		if err == nil {
			vals = append(vals, d)
		}
	}

	for i := range vals {
		for j := range vals {
			a, b := vals[i], vals[j]
			assert.Equal(t, a.Cmp(b), strings.Compare(a.SQLText(), b.SQLText()), "%s vs %s", a.String(), b.String())
		}
	}

	sorted := append([]bqsql.Decimal{}, vals...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].SQLText() < sorted[j].SQLText() })

	for i := 1; i < len(sorted); i++ {
		assert.LessOrEqual(t, sorted[i-1].Cmp(sorted[i]), 0)
	}
}

func TestDecimalSQLTextRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		kind bqsql.Kind
	}{
		{"zero", "0", bqsql.KindNumeric},
		{"neg", "-12.5", bqsql.KindNumeric},
		{"nummax", numMax, bqsql.KindNumeric},
		{"numnegmax", "-" + numMax, bqsql.KindNumeric},
		{"bigmax", bigMax, bqsql.KindBigNumeric},
		{"bigmin", bigMin, bqsql.KindBigNumeric},
		{"bigtiny", "0.00000000000000000000000000000000000001", bqsql.KindBigNumeric},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := dec(t, tt.kind, tt.in)
			txt := d.SQLText()
			assert.True(t, bqsql.IsSQLText(txt))

			back, err := bqsql.DecodeSQLText(txt, tt.kind)
			require.NoError(t, err)
			assert.Equal(t, d.String(), back.String())
		})
	}

	same := dec(t, bqsql.KindNumeric, "2.5")
	wide := dec(t, bqsql.KindBigNumeric, "2.5")
	assert.Equal(t, same.SQLText(), wide.SQLText())
}

func TestDecimalSQLTextRejects(t *testing.T) {
	t.Parallel()

	for _, s := range []string{"", "1.5", "#12", "#" + strings.Repeat("g", 64), strings.Repeat("a", 65)} {
		assert.False(t, bqsql.IsSQLText(s), s)

		_, err := bqsql.DecodeSQLText(s, bqsql.KindNumeric)
		require.ErrorIs(t, err, bqsql.ErrDecimal)
	}

	_, err := bqsql.DecodeSQLText(dec(t, bqsql.KindBigNumeric, bigMax).SQLText(), bqsql.KindNumeric)
	require.ErrorIs(t, err, bqsql.ErrDecimal)
}

func TestParseNumericText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
		fail bool
	}{
		{"plain", "123.45", "123.45", false},
		{"commas", "1,234,567.891", "1234567.891", false},
		{"space", "  12  ", "12", false},
		{"signspace", "-  12.3", "-12.3", false},
		{"trailsign", "12.3-", "-12.3", false},
		{"plussign", "+5", "5", false},
		{"exp", "1.5e2", "150", false},
		{"expneg", "15E-1", "1.5", false},
		{"round", "0.0000000005", "0.000000001", false},
		{"onlycommas", ",,,.1234", "", true},
		{"midspace", "1  23.4 5", "", true},
		{"nodigits", "  e1 ", "", true},
		{"twosigns", "  - 12.3 - ", "", true},
		{"currency", "$12.34", "", true},
		{"commaafter", "1.2,3", "", true},
		{"empty", "", "", true},
		{"over", "1e30", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d, err := bqsql.ParseNumericText(bqsql.KindNumeric, tt.in)
			if tt.fail {
				require.ErrorIs(t, err, bqsql.ErrDecimal)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, d.String())
		})
	}
}

func TestDecimalMath(t *testing.T) {
	t.Parallel()

	n, b := bqsql.KindNumeric, bqsql.KindBigNumeric

	tests := []struct {
		name string
		fn   string
		x, y string
		want string
		kind bqsql.Kind
	}{
		{"sqrt4", "sqrt", "4", "", "2", n},
		{"sqrt2", "sqrt", "2", "", "1.414213562", n},
		{"sqrt0", "sqrt", "0", "", "0", n},
		{"sqrtbig", "sqrt", "2", "", "1.41421356237309504880168872420969807857", b},
		{"exp1", "exp", "1", "", "2.718281828", n},
		{"exp0", "exp", "0", "", "1", n},
		{"expneg", "exp", "-1", "", "0.367879441", n},
		{"expunder", "exp", "-500", "", "0", n},
		{"ln10", "ln", "10", "", "2.302585093", n},
		{"ln1", "ln", "1", "", "0", n},
		{"lnbig", "ln", "2", "", "0.69314718055994530941723212145817656808", b},
		{"log10", "log10", "1000", "", "3", n},
		{"log10frac", "log10", "0.001", "", "-3", n},
		{"log2", "log", "8", "2", "3", n},
		{"log100", "log", "100", "10", "2", n},
		{"pow", "pow", "2", "10", "1024", n},
		{"powneg", "pow", "2", "-2", "0.25", n},
		{"powfrac", "pow", "2", "0.5", "1.414213562", n},
		{"powzero", "pow", "0", "0", "1", n},
		{"powzerobase", "pow", "0", "5", "0", n},
		{"pownegbase", "pow", "-2", "3", "-8", n},
		{"powunder", "pow", "10", "-30", "0", n},
		{"powbig", "pow", "2", "0.5", "1.41421356237309504880168872420969807857", b},
		{"intdiv", "intdiv", "7.5", "2", "3", n},
		{"intdivneg", "intdiv", "-7.5", "2", "-3", n},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			x := dec(t, tt.kind, tt.x)

			var (
				got bqsql.Decimal
				err error
			)

			switch tt.fn {
			case "sqrt":
				got, err = x.Sqrt()
			case "exp":
				got, err = x.Exp()
			case "ln":
				got, err = x.Ln()
			case "log10":
				got, err = x.Log10()
			case "log":
				got, err = x.Log(dec(t, tt.kind, tt.y))
			case "pow":
				got, err = x.Pow(dec(t, tt.kind, tt.y))
			default:
				got, err = x.IntDiv(dec(t, tt.kind, tt.y))
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got.String())
		})
	}
}

func TestDecimalMathErrors(t *testing.T) {
	t.Parallel()

	n := bqsql.KindNumeric
	neg, zero, two := dec(t, n, "-1"), dec(t, n, "0"), dec(t, n, "2")

	tests := []struct {
		run  func() error
		name string
	}{
		{func() error {
			_, err := neg.Sqrt()

			return err
		}, "sqrtneg"},
		{func() error {
			_, err := zero.Ln()

			return err
		}, "lnzero"},
		{func() error {
			_, err := neg.Log10()

			return err
		}, "log10neg"},
		{func() error {
			_, err := two.Log(dec(t, n, "1"))

			return err
		}, "logbase1"},
		{func() error {
			_, err := neg.Log(two)

			return err
		}, "logneg"},
		{func() error {
			_, err := dec(t, n, "100").Exp()

			return err
		}, "expover"},
		{func() error {
			_, err := neg.Pow(dec(t, n, "0.5"))

			return err
		}, "powneg"},
		{func() error {
			_, err := zero.Pow(neg)

			return err
		}, "powzero"},
		{func() error {
			_, err := dec(t, n, "10").Pow(dec(t, n, "40"))

			return err
		}, "powover"},
		{func() error {
			_, err := two.IntDiv(zero)

			return err
		}, "intdivzero"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, tt.run(), bqsql.ErrDecimal)
		})
	}
}

func TestSQLiteValueBoundary(t *testing.T) {
	t.Parallel()

	n := bqsql.Field{Name: "n", Type: "NUMERIC"}
	b := bqsql.Field{Name: "b", Type: "BIGNUMERIC"}

	enc := bqsql.ToSQLite(b, "0.00000000000000000000000000000000000001")
	txt, ok := enc.(string)
	require.True(t, ok)
	assert.True(t, bqsql.IsSQLText(txt))

	back, err := bqsql.FromSQLite(b, txt)
	require.NoError(t, err)
	assert.Equal(t, "0.00000000000000000000000000000000000001", back)

	for _, bad := range []any{float64(1.5), int64(2), "1.5", []byte("x")} {
		_, err = bqsql.FromSQLite(n, bad)
		require.ErrorIs(t, err, bqsql.ErrValue)
	}

	nested := bqsql.Field{Name: "a", Type: "NUMERIC", Mode: bqsql.ModeRepeated}
	text, ok := bqsql.ToSQLite(nested, []any{"1.5", "-2"}).(string)
	require.True(t, ok)
	assert.NotContains(t, text, "1.5")

	got, err := bqsql.FromSQLite(nested, text)
	require.NoError(t, err)
	assert.Equal(t, []any{"1.5", "-2"}, got)
	assert.JSONEq(t, `["1.5","-2"]`, bqsql.JSONText(nested, []any{"1.5", "-2"}))
}
