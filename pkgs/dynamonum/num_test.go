package dynamonum_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/dynamonum"
)

func TestParseCanonical(t *testing.T) {
	t.Parallel()

	tests := []struct{ in, want string }{
		{"1", "1"},
		{"1.0", "1"},
		{"1.10", "1.1"},
		{"-0", "0"},
		{"0.000", "0"},
		{"1E+3", "1000"},
		{"1e3", "1000"},
		{"12.5e-2", "0.125"},
		{"007", "7"},
		{"-1.50", "-1.5"},
		{".5", "0.5"},
		{"100", "100"},
		{"1E-130", "0." + zeros(129) + "1"},
		{"99999999999999999999999999999999999999", "99999999999999999999999999999999999999"},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()

			x, err := dynamonum.Parse(tt.in)
			require.NoError(t, err)
			assert.Equal(t, tt.want, x.String())
		})
	}
}

func zeros(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = '0'
	}

	return string(b)
}

func TestParseErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err error
		in  string
	}{
		{dynamonum.ErrInvalid, ""},
		{dynamonum.ErrInvalid, "abc"},
		{dynamonum.ErrInvalid, "1e"},
		{dynamonum.ErrInvalid, "1.2.3"},
		{dynamonum.ErrInvalid, "NaN"},
		{dynamonum.ErrInvalid, "Inf"},
		{dynamonum.ErrTooManyDigits, "123456789012345678901234567890123456789"},
		{dynamonum.ErrOverflow, "1E+126"},
		{dynamonum.ErrUnderflow, "1E-131"},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()

			_, err := dynamonum.Parse(tt.in)
			require.ErrorIs(t, err, tt.err)
		})
	}

	_, err := dynamonum.Parse("9.9999999999999999999999999999999999999E+125")
	require.NoError(t, err)
}

func TestCmp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		a, b string
		want int
	}{
		{"1", "1.0", 0},
		{"1E+3", "1000", 0},
		{"-0", "0", 0},
		{"12345678901234567890123456789012345678", "12345678901234567890123456789012345679", -1},
		{"-12345678901234567890123456789012345678", "-12345678901234567890123456789012345679", 1},
		{"2", "10", -1},
		{"-2", "-10", 1},
		{"-1", "1", -1},
		{"0.5", "0", 1},
		{"-0.5", "0", -1},
		{"1.5", "1.25", 1},
		{"1", "12", -1},
	}

	for _, tt := range tests {
		t.Run(tt.a+"_"+tt.b, func(t *testing.T) {
			t.Parallel()

			a, err := dynamonum.Parse(tt.a)
			require.NoError(t, err)

			b, err := dynamonum.Parse(tt.b)
			require.NoError(t, err)
			assert.Equal(t, tt.want, a.Cmp(b))
			assert.Equal(t, -tt.want, b.Cmp(a))
		})
	}
}

func TestAddSub(t *testing.T) {
	t.Parallel()

	maxNum := "99999999999999999999999999999999999999"
	tests := []struct {
		err            error
		a, b, add, sub string
	}{
		{a: "1", b: "2", add: "3", sub: "-1"},
		{a: "0.1", b: "0.2", add: "0.3", sub: "-0.1"},
		{
			a:   "12345678901234567890123456789012345678",
			b:   "1",
			add: "12345678901234567890123456789012345679",
			sub: "12345678901234567890123456789012345677",
		},
		{a: "1E+10", b: "1", add: "10000000001", sub: "9999999999"},
		{a: "1.10", b: "0", add: "1.1", sub: "1.1"},
		{a: maxNum, b: "0.5", err: dynamonum.ErrTooManyDigits, sub: "99999999999999999999999999999999999998.5"},
		{a: "5E+125", b: "5E+125", err: dynamonum.ErrOverflow, sub: "0"},
		{a: "1E-130", b: "1E-130", add: "0." + zeros(129) + "2", sub: "0"},
		{a: "1E-130", b: "-1E-130", add: "0", sub: "0." + zeros(129) + "2"},
	}

	for _, tt := range tests {
		t.Run(tt.a+"_"+tt.b, func(t *testing.T) {
			t.Parallel()

			a, err := dynamonum.Parse(tt.a)
			require.NoError(t, err)

			b, err := dynamonum.Parse(tt.b)
			require.NoError(t, err)

			got, err := dynamonum.Add(a, b)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.add, got.String())
			}

			got, err = dynamonum.Sub(a, b)
			if tt.sub == "" {
				return
			}

			if tt.err != nil && tt.sub == "0" {
				return
			}

			if tt.a == maxNum {
				require.ErrorIs(t, err, dynamonum.ErrTooManyDigits)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.sub, got.String())
		})
	}
}

func BenchmarkParseCmp(b *testing.B) {
	lo, _ := dynamonum.Parse("100")
	b.ReportAllocs()

	for b.Loop() {
		x, _ := dynamonum.Parse("12345")
		_ = x.Cmp(lo)
	}
}

func TestEqualValuesAreComparable(t *testing.T) {
	t.Parallel()

	tests := []struct{ a, b string }{
		{"1", "1.0"},
		{"1000", "1E+3"},
		{"-5", "-5.00"},
		{"0", "-0.0"},
		{"1E+20", "100000000000000000000"},
		{"0.25", "25E-2"},
	}

	for _, tt := range tests {
		t.Run(tt.a+"_"+tt.b, func(t *testing.T) {
			t.Parallel()

			a, err := dynamonum.Parse(tt.a)
			require.NoError(t, err)

			b, err := dynamonum.Parse(tt.b)
			require.NoError(t, err)
			assert.Equal(t, a, b)
		})
	}

	got, err := dynamonum.Add(dynamonum.FromInt64(999999999999999999), dynamonum.FromInt64(1))
	require.NoError(t, err)
	assert.Equal(t, "1000000000000000000", got.String())
}
