package bqsql_test

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const (
	numMax   = "99999999999999999999999999999.999999999"
	bigMax   = "578960446186580977117854925043439539266.34992332820282019728792003956564819967"
	bigOver  = "578960446186580977117854925043439539266.34992332820282019728792003956564819968"
	bigUnder = "-578960446186580977117854925043439539266.34992332820282019728792003956564819969"
	bigMin   = "-578960446186580977117854925043439539266.34992332820282019728792003956564819968"
)

func dec(t *testing.T, k bqsql.Kind, s string) bqsql.Decimal {
	t.Helper()

	d, err := bqsql.ParseDecimal(k, s)
	require.NoError(t, err)

	return d
}

func TestDecimalParseFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
		kind bqsql.Kind
	}{
		{"int", "42", "42", bqsql.KindNumeric},
		{"trim", "1.500000000", "1.5", bqsql.KindNumeric},
		{"neg", "-0.25", "-0.25", bqsql.KindNumeric},
		{"negzero", "-0.0", "0", bqsql.KindNumeric},
		{"negtiny", "-0.0000000004", "0", bqsql.KindNumeric},
		{"plus", "+7", "7", bqsql.KindNumeric},
		{"leadpoint", ".5", "0.5", bqsql.KindNumeric},
		{"trailpoint", "5.", "5", bqsql.KindNumeric},
		{"exp", "1.5e3", "1500", bqsql.KindNumeric},
		{"expneg", "25E-2", "0.25", bqsql.KindNumeric},
		{"space", "  3.14 ", "3.14", bqsql.KindNumeric},
		{"halfup", "0.0000000005", "0.000000001", bqsql.KindNumeric},
		{"halfupneg", "-0.0000000005", "-0.000000001", bqsql.KindNumeric},
		{"halfdown", "0.0000000004999", "0", bqsql.KindNumeric},
		{"max", numMax, numMax, bqsql.KindNumeric},
		{"min", "-" + numMax, "-" + numMax, bqsql.KindNumeric},
		{"hugeneg", "1e-999", "0", bqsql.KindNumeric},
		{"bigmax", bigMax, bigMax, bqsql.KindBigNumeric},
		{"bigmin", bigMin, bigMin, bqsql.KindBigNumeric},
		{
			"bigfrac", "0.123456789012345678901234567890123456785",
			"0.12345678901234567890123456789012345679", bqsql.KindBigNumeric,
		},
		{"bigsmall", "1e-38", "0.00000000000000000000000000000000000001", bqsql.KindBigNumeric},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, dec(t, tt.kind, tt.in).String())
		})
	}
}

func TestDecimalParseErrors(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("9", 600)

	tests := []struct {
		name string
		in   string
		kind bqsql.Kind
	}{
		{"empty", "", bqsql.KindNumeric},
		{"blank", "   ", bqsql.KindNumeric},
		{"point", ".", bqsql.KindNumeric},
		{"sign", "-", bqsql.KindNumeric},
		{"text", "abc", bqsql.KindNumeric},
		{"nan", "NaN", bqsql.KindNumeric},
		{"inf", "Infinity", bqsql.KindNumeric},
		{"twopoints", "1.2.3", bqsql.KindNumeric},
		{"noexp", "1e", bqsql.KindNumeric},
		{"badexp", "1e+x", bqsql.KindNumeric},
		{"hugeexp", "1e99999", bqsql.KindNumeric},
		{"toolong", long, bqsql.KindNumeric},
		{"over", "100000000000000000000000000000", bqsql.KindNumeric},
		{"overneg", "-1e29", bqsql.KindNumeric},
		{"expover", "1e400", bqsql.KindNumeric},
		{"bigover", bigOver, bqsql.KindBigNumeric},
		{"bigunder", bigUnder, bqsql.KindBigNumeric},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := bqsql.ParseDecimal(tt.kind, tt.in)
			require.ErrorIs(t, err, bqsql.ErrDecimal)
		})
	}
}

func TestDecimalArithmetic(t *testing.T) {
	t.Parallel()

	n, b := bqsql.KindNumeric, bqsql.KindBigNumeric

	tests := []struct {
		name string
		op   string
		x, y string
		want string
		kind bqsql.Kind
	}{
		{"add", "+", "0.1", "0.2", "0.3", n},
		{"addneg", "+", "-0.1", "0.05", "-0.05", n},
		{
			"add38", "+", "12345678901234567890123456789.123456789", "0.000000001",
			"12345678901234567890123456789.12345679", n,
		},
		{"sub", "-", "0.3", "0.1", "0.2", n},
		{"subzero", "-", "1.5", "1.5", "0", n},
		{"mul", "*", "0.1", "0.1", "0.01", n},
		{"mulround", "*", "0.000000001", "0.5", "0.000000001", n},
		{"mulrounddown", "*", "0.000000001", "0.4", "0", n},
		{"mulroundneg", "*", "-0.000000001", "0.5", "-0.000000001", n},
		{"quothird", "/", "1", "3", "0.333333333", n},
		{"quotwothird", "/", "2", "3", "0.666666667", n},
		{"quoneg", "/", "-1", "3", "-0.333333333", n},
		{"quohalf", "/", "1", "2", "0.5", n},
		{"mod", "%", "7.5", "2", "1.5", n},
		{"modneg", "%", "-7.5", "2", "-1.5", n},
		{"modnegdiv", "%", "7.5", "-2", "1.5", n},
		{"bigquo", "/", "1", "3", "0.33333333333333333333333333333333333333", b},
		{"bigmul", "*", "1e19", "1e19", "100000000000000000000000000000000000000", b},
		{"mixed", "+", "0.1", "0.2", "0.3", b},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			x, y := dec(t, tt.kind, tt.x), dec(t, tt.kind, tt.y)

			var (
				got bqsql.Decimal
				err error
			)

			switch tt.op {
			case "+":
				got, err = x.Add(y)
			case "-":
				got, err = x.Sub(y)
			case "*":
				got, err = x.Mul(y)
			case "/":
				got, err = x.Quo(y)
			default:
				got, err = x.Mod(y)
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got.String())
			assert.Equal(t, tt.kind, got.Kind())
		})
	}
}

func TestDecimalMixedKindPromotes(t *testing.T) {
	t.Parallel()

	tiny := dec(t, bqsql.KindBigNumeric, "0.00000000000000000000000000000000000001")
	got, err := dec(t, bqsql.KindNumeric, "1.5").Add(tiny)
	require.NoError(t, err)
	assert.Equal(t, bqsql.KindBigNumeric, got.Kind())
	assert.Equal(t, "1.50000000000000000000000000000000000001", got.String())
}

func TestDecimalErrors(t *testing.T) {
	t.Parallel()

	maxN := dec(t, bqsql.KindNumeric, numMax)
	one := dec(t, bqsql.KindNumeric, "0.000000001")
	zero := dec(t, bqsql.KindNumeric, "0")
	bmax := dec(t, bqsql.KindBigNumeric, bigMax)
	bmin := dec(t, bqsql.KindBigNumeric, bigMin)

	tests := []struct {
		run  func() error
		name string
		msg  string
	}{
		{func() error {
			_, err := maxN.Add(one)

			return err
		}, "addover", "numeric overflow: " + numMax + " + 0.000000001"},
		{func() error {
			_, err := maxN.Mul(maxN)

			return err
		}, "mulover", "numeric overflow"},
		{func() error {
			_, err := bmin.Neg()

			return err
		}, "negover", "BIGNUMERIC overflow"},
		{func() error {
			_, err := bmin.Abs()

			return err
		}, "absover", "BIGNUMERIC overflow"},
		{func() error {
			_, err := bmax.Add(bmax)

			return err
		}, "bigaddover", "BIGNUMERIC overflow"},
		{func() error {
			_, err := one.Quo(zero)

			return err
		}, "divzero", "division by zero: 0.000000001 / 0"},
		{func() error {
			_, err := one.Mod(zero)

			return err
		}, "modzero", "division by zero"},
		{func() error {
			_, err := bmax.As(bqsql.KindNumeric)

			return err
		}, "narrow", "numeric overflow"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.run()
			require.ErrorIs(t, err, bqsql.ErrDecimal)
			assert.Contains(t, err.Error(), tt.msg)
		})
	}
}

func TestDecimalNegAbsSignCmp(t *testing.T) {
	t.Parallel()

	x := dec(t, bqsql.KindNumeric, "-2.5")
	neg, err := x.Neg()
	require.NoError(t, err)
	assert.Equal(t, "2.5", neg.String())

	abs, err := x.Abs()
	require.NoError(t, err)
	assert.Equal(t, "2.5", abs.String())
	assert.Equal(t, -1, x.Sign())
	assert.Equal(t, 0, dec(t, bqsql.KindNumeric, "0").Sign())
	assert.Equal(t, 1, neg.Sign())

	assert.Negative(t, x.Cmp(neg))
	assert.Positive(t, neg.Cmp(x))
	assert.Zero(t, neg.Cmp(dec(t, bqsql.KindBigNumeric, "2.5")))
	above := dec(t, bqsql.KindBigNumeric, "1.00000000000000000000000000000000000001")
	assert.Negative(t, dec(t, bqsql.KindNumeric, "1").Cmp(above))

	z, err := dec(t, bqsql.KindNumeric, "0").Neg()
	require.NoError(t, err)
	assert.Equal(t, "0", z.String())
}

func TestDecimalRounding(t *testing.T) {
	t.Parallel()

	away, even := bqsql.RoundHalfAwayFromZero, bqsql.RoundHalfEven

	tests := []struct {
		name   string
		in     string
		want   string
		digits int
		mode   bqsql.RoundMode
	}{
		{"away", "2.5", "3", 0, away},
		{"awayneg", "-2.5", "-3", 0, away},
		{"even", "2.5", "2", 0, even},
		{"even3", "3.5", "4", 0, even},
		{"evenneg", "-2.5", "-2", 0, even},
		{"evenabove", "2.50001", "3", 0, even},
		{"two", "1.125", "1.13", 2, away},
		{"twoneg", "-1.125", "-1.13", 2, away},
		{"eventwo", "1.125", "1.12", 2, even},
		{"tens", "1250", "1300", -2, away},
		{"tensneg", "-1250", "-1300", -2, away},
		{"tenseven", "1250", "1200", -2, even},
		{"wide", "1.5", "1.5", 20, away},
		{"tiny", "0.4", "0", -400, away},
		{"tonegzero", "-0.4", "0", 0, away},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := dec(t, bqsql.KindNumeric, tt.in).Round(tt.digits, tt.mode)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.String())
		})
	}

	_, err := dec(t, bqsql.KindNumeric, "99999999999999999999999999999.5").Round(0, away)
	require.ErrorIs(t, err, bqsql.ErrDecimal)
}

func TestDecimalTruncCeilFloor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in          string
		trunc1      string
		ceil, floor string
	}{
		{"2.75", "2.7", "3", "2"},
		{"-2.75", "-2.7", "-2", "-3"},
		{"5", "5", "5", "5"},
		{"-0.5", "-0.5", "0", "-1"},
		{"0.5", "0.5", "1", "0"},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()

			d := dec(t, bqsql.KindNumeric, tt.in)
			assert.Equal(t, tt.trunc1, d.Trunc(1).String())

			c, err := d.Ceil()
			require.NoError(t, err)
			assert.Equal(t, tt.ceil, c.String())

			f, err := d.Floor()
			require.NoError(t, err)
			assert.Equal(t, tt.floor, f.String())
		})
	}

	assert.Equal(t, "0", dec(t, bqsql.KindNumeric, "0.5").Trunc(-500).String())
}

func TestDecimalConversions(t *testing.T) {
	t.Parallel()

	i, err := bqsql.DecimalFromInt(bqsql.KindNumeric, math.MinInt64)
	require.NoError(t, err)
	assert.Equal(t, "-9223372036854775808", i.String())

	back, err := i.Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(math.MinInt64), back)

	r, err := dec(t, bqsql.KindNumeric, "2.5").Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(3), r)

	r, err = dec(t, bqsql.KindNumeric, "-2.5").Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(-3), r)

	_, err = dec(t, bqsql.KindNumeric, "9223372036854775807.5").Int64()
	require.ErrorIs(t, err, bqsql.ErrDecimal)

	f, err := bqsql.DecimalFromFloat(bqsql.KindNumeric, 0.1)
	require.NoError(t, err)
	assert.Equal(t, "0.1", f.String())

	f, err = bqsql.DecimalFromFloat(bqsql.KindBigNumeric, 0.1)
	require.NoError(t, err)
	assert.Equal(t, "0.10000000000000000555111512312578270212", f.String())

	f, err = bqsql.DecimalFromFloat(bqsql.KindNumeric, -1e-10)
	require.NoError(t, err)
	assert.Equal(t, "0", f.String())

	for _, bad := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), 1e30} {
		_, err = bqsql.DecimalFromFloat(bqsql.KindNumeric, bad)
		require.ErrorIs(t, err, bqsql.ErrDecimal)
	}

	assert.InDelta(t, 0.3, dec(t, bqsql.KindNumeric, "0.3").Float64(), 0)
	assert.InDelta(t, -1.5e20, dec(t, bqsql.KindNumeric, "-150000000000000000000").Float64(), 0)

	wide, err := dec(t, bqsql.KindNumeric, "1.5").As(bqsql.KindBigNumeric)
	require.NoError(t, err)
	assert.Equal(t, bqsql.KindBigNumeric, wide.Kind())

	narrow, err := dec(t, bqsql.KindBigNumeric, "0.0000000005").As(bqsql.KindNumeric)
	require.NoError(t, err)
	assert.Equal(t, "0.000000001", narrow.String())
}

func TestDecimalConstrain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
		p, s int
		kind bqsql.Kind
		fail bool
	}{
		{"round", "1.125", "1.13", 5, 2, bqsql.KindNumeric, false},
		{"fits", "999.99", "999.99", 5, 2, bqsql.KindNumeric, false},
		{"over", "1000", "", 5, 2, bqsql.KindNumeric, true},
		{"roundover", "999.995", "", 5, 2, bqsql.KindNumeric, true},
		{"scale0", "2.5", "3", 3, 0, bqsql.KindNumeric, false},
		{"negover", "-1000", "", 5, 2, bqsql.KindNumeric, true},
		{"badscale", "1", "", 5, 10, bqsql.KindNumeric, true},
		{"badprec", "1", "", 40, 9, bqsql.KindNumeric, true},
		{"badzero", "1", "", 0, 0, bqsql.KindNumeric, true},
		{"precbelowscale", "1", "", 2, 3, bqsql.KindNumeric, true},
		{"bigok", "1.5", "1.5", 76, 38, bqsql.KindBigNumeric, false},
		{"bigover", "1000", "", 40, 38, bqsql.KindBigNumeric, true},
		{"bigscale", "1", "", 10, 39, bqsql.KindBigNumeric, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := dec(t, tt.kind, tt.in).Constrain(tt.p, tt.s)
			if tt.fail {
				require.ErrorIs(t, err, bqsql.ErrDecimal)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got.String())
		})
	}
}

func TestDecimalZeroValue(t *testing.T) {
	t.Parallel()

	var z bqsql.Decimal

	assert.Equal(t, "0", z.String())
	assert.Equal(t, bqsql.KindNumeric, z.Kind())
	assert.Zero(t, z.Sign())
	assert.InDelta(t, 0.0, z.Float64(), 0)
}
