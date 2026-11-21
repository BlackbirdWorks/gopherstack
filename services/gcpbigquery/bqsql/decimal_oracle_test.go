package bqsql_test

import (
	"math/big"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

type oracleKind struct {
	min, max *big.Int
	scale    int
	kind     bqsql.Kind
}

func oracleKinds() map[string]oracleKind {
	p10 := func(n int) *big.Int { return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil) }
	lim := new(big.Int).Lsh(big.NewInt(1), 255)

	return map[string]oracleKind{
		"numeric": {
			min: new(big.Int).Neg(new(big.Int).Sub(p10(38), big.NewInt(1))),
			max: new(big.Int).Sub(p10(38), big.NewInt(1)), scale: 9, kind: bqsql.KindNumeric,
		},
		"bignumeric": {
			min: new(big.Int).Neg(lim), max: new(big.Int).Sub(lim, big.NewInt(1)), scale: 38,
			kind: bqsql.KindBigNumeric,
		},
	}
}

func ratOf(t *testing.T, s string) *big.Rat {
	t.Helper()

	r, ok := new(big.Rat).SetString(s)
	require.True(t, ok, s)

	return r
}

func roundHalfAway(r *big.Rat) *big.Int {
	num, den := new(big.Int).Set(r.Num()), r.Denom()
	neg := num.Sign() < 0
	num.Abs(num)

	q, rem := new(big.Int).QuoRem(num, den, new(big.Int))
	if rem.Lsh(rem, 1).Cmp(den) >= 0 {
		q.Add(q, big.NewInt(1))
	}

	if neg {
		q.Neg(q)
	}

	return q
}

func (k oracleKind) unscaled(r *big.Rat) (*big.Int, bool) {
	n := roundHalfAway(
		new(big.Rat).Mul(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(k.scale)), nil))),
	)

	return n, n.Cmp(k.min) >= 0 && n.Cmp(k.max) <= 0
}

func (k oracleKind) canonical(n *big.Int) string {
	r := new(big.Rat).SetFrac(n, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(k.scale)), nil))
	s := r.FloatString(k.scale)

	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}

	if s == "-0" {
		return "0"
	}

	return s
}

func truncRat(r *big.Rat) *big.Rat {
	return new(big.Rat).SetInt(new(big.Int).Quo(r.Num(), r.Denom()))
}

func floorRat(r *big.Rat) *big.Rat {
	q := new(big.Int).Div(r.Num(), r.Denom())

	return new(big.Rat).SetInt(q)
}

type binOracle struct {
	got  func(a, b bqsql.Decimal) (bqsql.Decimal, error)
	want func(a, b *big.Rat) (*big.Rat, bool)
	name string
}

func binOracles() []binOracle {
	return []binOracle{
		{bqsql.Decimal.Add, func(a, b *big.Rat) (*big.Rat, bool) { return new(big.Rat).Add(a, b), true }, "add"},
		{bqsql.Decimal.Sub, func(a, b *big.Rat) (*big.Rat, bool) { return new(big.Rat).Sub(a, b), true }, "sub"},
		{bqsql.Decimal.Mul, func(a, b *big.Rat) (*big.Rat, bool) { return new(big.Rat).Mul(a, b), true }, "mul"},
		{bqsql.Decimal.Quo, func(a, b *big.Rat) (*big.Rat, bool) {
			if b.Sign() == 0 {
				return nil, false
			}

			return new(big.Rat).Quo(a, b), true
		}, "quo"},
		{bqsql.Decimal.Mod, func(a, b *big.Rat) (*big.Rat, bool) {
			if b.Sign() == 0 {
				return nil, false
			}

			q := truncRat(new(big.Rat).Quo(a, b))

			return new(big.Rat).Sub(a, new(big.Rat).Mul(q, b)), true
		}, "mod"},
		{bqsql.Decimal.IntDiv, func(a, b *big.Rat) (*big.Rat, bool) {
			if b.Sign() == 0 {
				return nil, false
			}

			return truncRat(new(big.Rat).Quo(a, b)), true
		}, "intdiv"},
	}
}

func checkBinary(t *testing.T, name string, ok oracleKind, op binOracle, as, bs string) {
	t.Helper()

	a, err := bqsql.ParseDecimal(ok.kind, as)
	require.NoError(t, err, as)

	b, err := bqsql.ParseDecimal(ok.kind, bs)
	require.NoError(t, err, bs)

	ra, rb := ratOf(t, a.String()), ratOf(t, b.String())
	want, valid := op.want(ra, rb)
	got, gerr := op.got(a, b)

	if !valid {
		require.Error(t, gerr, "%s %s %s %s", name, op.name, as, bs)

		return
	}

	n, inRange := ok.unscaled(want)
	if !inRange {
		require.Error(t, gerr, "%s %s(%s, %s) should overflow", name, op.name, as, bs)
		assert.ErrorIs(t, gerr, bqsql.ErrDecimal)

		return
	}

	require.NoError(t, gerr, "%s %s(%s, %s)", name, op.name, as, bs)
	assert.Equal(t, ok.canonical(n), got.String(), "%s %s(%s, %s)", name, op.name, as, bs)
	assert.Equal(t, ok.kind, got.Kind())
}

func oracleHardInputs() map[string][]string {
	return map[string][]string{
		"numeric": {
			"0", "1", "-1", "0.5", "-0.5", "0.000000001", "-0.000000001", "0.0000000005", "0.000000005", "2.5", "-2.5",
			"0.3333333333", "99999999999999999999999999999.999999999", "-99999999999999999999999999999.999999999",
			"99999999999999999999999999999.999999998", "1e20", "-1e-9", "123456789.123456789", "3", "-3", "7", "-7",
			"0.1", "0.2", "1e19", "4.5", "-4.5", "1.0000000005", "-1.0000000005",
		},
		"bignumeric": {
			"0", "1", "-1", "0.5", "-0.5", "0.00000000000000000000000000000000000001",
			"-0.00000000000000000000000000000000000001", "0.000000000000000000000000000000000000005",
			"578960446186580977117854925043439539266.34992332820282019728792003956564819967",
			"-578960446186580977117854925043439539266.34992332820282019728792003956564819968",
			"578960446186580977117854925043439539266.34992332820282019728792003956564819966",
			"-578960446186580977117854925043439539266.34992332820282019728792003956564819967",
			"1e19", "1e20", "-1e19", "3", "-3", "7", "-7", "2.5", "-2.5", "0.33333333333333333333333333333333333333",
			"99999999999999999999999999999999999999.99999999999999999999999999999999999999", "12345678901234567890.5",
		},
	}
}

func TestDecimalOracleHardCases(t *testing.T) {
	t.Parallel()

	for name, ok := range oracleKinds() {
		inputs := oracleHardInputs()[name]

		for _, op := range binOracles() {
			t.Run(name+"/"+op.name, func(t *testing.T) {
				t.Parallel()

				for _, a := range inputs {
					for _, b := range inputs {
						checkBinary(t, name, ok, op, a, b)
					}
				}
			})
		}
	}
}

func oracleRandom(r *rand.Rand, ok oracleKind) string {
	maxInt := 29
	if ok.scale == 38 {
		maxInt = 38
	}

	intDigits := r.IntN(maxInt + 1)
	if r.IntN(4) == 0 {
		intDigits = r.IntN(4)
	}

	digits := func(n int) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte('0' + r.IntN(10))
		}

		return string(b)
	}

	s := "0"
	if intDigits > 0 {
		s = digits(intDigits)
	}

	if frac := r.IntN(ok.scale + 1); frac > 0 {
		s += "." + digits(frac)
	}

	if r.IntN(2) == 0 {
		s = "-" + s
	}

	return s
}

func TestDecimalOracleRandom(t *testing.T) {
	t.Parallel()

	for name, ok := range oracleKinds() {
		for _, op := range binOracles() {
			t.Run(name+"/"+op.name, func(t *testing.T) {
				t.Parallel()

				r := rand.New(rand.NewPCG(7, uint64(len(op.name))))

				for range 600 {
					checkBinary(t, name, ok, op, oracleRandom(r, ok), oracleRandom(r, ok))
				}
			})
		}
	}
}

func TestDecimalOracleUnary(t *testing.T) {
	t.Parallel()

	type unary struct {
		got  func(d bqsql.Decimal) (bqsql.Decimal, error)
		want func(r *big.Rat) *big.Rat
		name string
	}

	ops := []unary{
		{bqsql.Decimal.Neg, func(r *big.Rat) *big.Rat { return new(big.Rat).Neg(r) }, "neg"},
		{bqsql.Decimal.Abs, func(r *big.Rat) *big.Rat { return new(big.Rat).Abs(r) }, "abs"},
		{func(d bqsql.Decimal) (bqsql.Decimal, error) { return d.Trunc(0), nil }, truncRat, "trunc0"},
		{bqsql.Decimal.Floor, floorRat, "floor"},
		{bqsql.Decimal.Ceil, func(r *big.Rat) *big.Rat {
			return new(big.Rat).Neg(floorRat(new(big.Rat).Neg(r)))
		}, "ceil"},
		{
			func(d bqsql.Decimal) (bqsql.Decimal, error) { return d.Round(0, bqsql.RoundHalfAwayFromZero) },
			func(r *big.Rat) *big.Rat {
				return new(big.Rat).SetInt(roundHalfAway(r))
			},
			"round0",
		},
	}

	for name, ok := range oracleKinds() {
		for _, op := range ops {
			t.Run(name+"/"+op.name, func(t *testing.T) {
				t.Parallel()

				for _, in := range oracleHardInputs()[name] {
					d, err := bqsql.ParseDecimal(ok.kind, in)
					require.NoError(t, err)

					want := op.want(ratOf(t, d.String()))
					n, inRange := ok.unscaled(want)
					got, gerr := op.got(d)

					if !inRange {
						require.Error(t, gerr, "%s(%s)", op.name, in)

						continue
					}

					require.NoError(t, gerr, "%s(%s)", op.name, in)
					assert.Equal(t, ok.canonical(n), got.String(), "%s(%s)", op.name, in)
				}
			})
		}
	}
}

func TestDecimalOracleRound(t *testing.T) {
	t.Parallel()

	for name, ok := range oracleKinds() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			for _, in := range oracleHardInputs()[name] {
				for digits := -40; digits <= ok.scale+2; digits++ {
					d, err := bqsql.ParseDecimal(ok.kind, in)
					require.NoError(t, err)

					unit := new(big.Rat).SetFrac(big.NewInt(1), big.NewInt(1))
					if digits >= 0 {
						unit.SetFrac(big.NewInt(1), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(digits)), nil))
					} else {
						unit.SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-digits)), nil))
					}

					scaled := new(big.Rat).Quo(ratOf(t, d.String()), unit)
					want := new(big.Rat).Mul(new(big.Rat).SetInt(roundHalfAway(scaled)), unit)
					wn, inRange := ok.unscaled(want)
					got, gerr := d.Round(digits, bqsql.RoundHalfAwayFromZero)

					if !inRange {
						require.Error(t, gerr, "ROUND(%s,%d)", in, digits)

						continue
					}

					require.NoError(t, gerr, "ROUND(%s,%d)", in, digits)
					assert.Equal(t, ok.canonical(wn), got.String(), "ROUND(%s,%d)", in, digits)

					tr := new(big.Rat).Mul(truncRat(new(big.Rat).Quo(ratOf(t, d.String()), unit)), unit)
					tn, _ := ok.unscaled(tr)
					assert.Equal(t, ok.canonical(tn), d.Trunc(digits).String(), "TRUNC(%s,%d)", in, digits)
				}
			}
		})
	}
}

func TestDecimalRangeBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		text  string
		kind  bqsql.Kind
		valid bool
	}{
		{"numeric max", "99999999999999999999999999999.999999999", bqsql.KindNumeric, true},
		{"numeric over", "100000000000000000000000000000", bqsql.KindNumeric, false},
		{"numeric min", "-99999999999999999999999999999.999999999", bqsql.KindNumeric, true},
		{"numeric under", "-100000000000000000000000000000", bqsql.KindNumeric, false},
		{"numeric round up to over", "99999999999999999999999999999.9999999995", bqsql.KindNumeric, false},
		{"big max", bigMax, bqsql.KindBigNumeric, true},
		{"big over", bigOver, bqsql.KindBigNumeric, false},
		{"big min", bigMin, bqsql.KindBigNumeric, true},
		{"big under", bigUnder, bqsql.KindBigNumeric, false},
		{
			"big documented max",
			"5.7896044618658097711785492504343953926634992332820282019728792003956564819967E+38",
			bqsql.KindBigNumeric,
			true,
		},
		{
			"big documented min",
			"-5.7896044618658097711785492504343953926634992332820282019728792003956564819968E+38",
			bqsql.KindBigNumeric,
			true,
		},
		{
			"big documented over",
			"5.7896044618658097711785492504343953926634992332820282019728792003956564819968E+38",
			bqsql.KindBigNumeric,
			false,
		},
		{"big round up to over", bigMax + "5", bqsql.KindBigNumeric, false},
		{"exp small", "1.5e-9", bqsql.KindNumeric, true},
		{"exp neg", "-12E+3", bqsql.KindNumeric, true},
		{"exp huge", "1e999", bqsql.KindNumeric, false},
		{"exp over bound", "1e1025", bqsql.KindNumeric, false},
		{"exp tiny", "1e-1000", bqsql.KindNumeric, true},
		{"over scale rounds", "0.0000000005", bqsql.KindNumeric, true},
		{"leading plus", "+1.5", bqsql.KindNumeric, true},
		{"bare dot", ".", bqsql.KindNumeric, false},
		{"dot only digits", ".5", bqsql.KindNumeric, true},
		{"trailing dot", "5.", bqsql.KindNumeric, true},
		{"two dots", "1.2.3", bqsql.KindNumeric, false},
		{"double sign", "--1", bqsql.KindNumeric, false},
		{"empty exponent", "1e", bqsql.KindNumeric, false},
		{"hex", "0x10", bqsql.KindNumeric, false},
		{"inf", "inf", bqsql.KindNumeric, false},
		{"nan", "NaN", bqsql.KindNumeric, false},
		{"underscore", "1_000", bqsql.KindNumeric, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d, err := bqsql.ParseDecimal(tt.kind, tt.text)
			if !tt.valid {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tt.name == "big max" || tt.name == "big min" || tt.name == "numeric max" {
				assert.Equal(t, tt.text, d.String())
			}
		})
	}

	d, err := bqsql.ParseDecimal(bqsql.KindBigNumeric, bigMax)
	require.NoError(t, err)

	_, err = d.Add(dec(t, bqsql.KindBigNumeric, "0.00000000000000000000000000000000000001"))
	require.Error(t, err)

	m, err := bqsql.ParseDecimal(bqsql.KindBigNumeric, bigMin)
	require.NoError(t, err)

	_, err = m.Neg()
	require.Error(t, err, "negating the minimum overflows")

	_, err = m.Abs()
	require.Error(t, err)

	_, err = m.Sub(dec(t, bqsql.KindBigNumeric, "0.00000000000000000000000000000000000001"))
	require.Error(t, err)
}

func TestDecimalCanonicalString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want string
		kind bqsql.Kind
	}{
		{"-0", "0", bqsql.KindNumeric},
		{"-0.0000000001", "0", bqsql.KindNumeric},
		{"1.500", "1.5", bqsql.KindNumeric},
		{"100", "100", bqsql.KindNumeric},
		{"0.000000001", "0.000000001", bqsql.KindNumeric},
		{"1e2", "100", bqsql.KindNumeric},
		{"-1.0E-1", "-0.1", bqsql.KindNumeric},
		{"00012.3400", "12.34", bqsql.KindBigNumeric},
		{
			"-0.00000000000000000000000000000000000001",
			"-0.00000000000000000000000000000000000001",
			bqsql.KindBigNumeric,
		},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, dec(t, tt.kind, tt.in).String())
		})
	}
}

func TestDecimalConstrainOracle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      string
		want    string
		p, s    int
		kind    bqsql.Kind
		wantErr bool
	}{
		{"1.125", "1.13", 5, 2, bqsql.KindNumeric, false},
		{"-1.125", "-1.13", 5, 2, bqsql.KindNumeric, false},
		{"999.995", "", 5, 2, bqsql.KindNumeric, true},
		{"999.994", "999.99", 5, 2, bqsql.KindNumeric, false},
		{"-999.995", "", 5, 2, bqsql.KindNumeric, true},
		{"12345", "12345", 5, 0, bqsql.KindNumeric, false},
		{"123456", "", 5, 0, bqsql.KindNumeric, true},
		{"0.5", "1", 1, 0, bqsql.KindNumeric, false},
		{"9.5", "", 1, 0, bqsql.KindNumeric, true},
		{"1", "1", 38, 9, bqsql.KindNumeric, false},
		{"1", "", 39, 9, bqsql.KindNumeric, true},
		{"1", "", 9, 9, bqsql.KindNumeric, true},
		{"0.1", "", 10, 10, bqsql.KindNumeric, true},
		{"0.1", "0.1", 9, 9, bqsql.KindNumeric, false},
		{"1", "", 10, 12, bqsql.KindNumeric, true},
		{
			"1.00000000000000000000000000000000000005",
			"1.00000000000000000000000000000000000005",
			76,
			38,
			bqsql.KindBigNumeric,
			false,
		},
		{"1", "1", 76, 38, bqsql.KindBigNumeric, false},
		{"1", "", 77, 38, bqsql.KindBigNumeric, true},
	}

	for _, tt := range tests {
		t.Run(tt.in+"/"+strconv.Itoa(tt.p)+"_"+strconv.Itoa(tt.s), func(t *testing.T) {
			t.Parallel()

			got, err := dec(t, tt.kind, tt.in).Constrain(tt.p, tt.s)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got.String())
		})
	}
}
