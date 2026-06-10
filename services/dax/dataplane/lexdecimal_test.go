package dataplane_test

import (
	"bytes"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dax/dataplane"
)

// TestLexDecimalRoundTrip verifies that numeric range-key values survive an
// encode/decode cycle through the lexicographic-decimal codec.
func TestLexDecimalRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "zero", in: "0", want: "0"},
		{name: "small positive", in: "42", want: "42"},
		{name: "small negative", in: "-42", want: "-42"},
		{name: "large", in: "1000000", want: "1000000"},
		{name: "fraction", in: "12.5", want: "12.5"},
		{name: "negative fraction", in: "-12.5", want: "-12.5"},
		{name: "many digits", in: "1234567890123456789", want: "1234567890123456789"},
		{name: "leading fraction", in: "0.001", want: "0.001"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := dataplane.LexDecimalRoundTripForTest(tc.in)
			if err != nil {
				t.Fatalf("LexDecimalRoundTripForTest(%q): %v", tc.in, err)
			}

			if !numericEqual(got, tc.want) {
				t.Fatalf("round-trip: got %q want %q", got, tc.want)
			}
		})
	}
}

// TestLexDecimalSortOrder verifies the encoding preserves numeric ordering, the
// property that makes lexdecimal valid as a sort-key encoding.
func TestLexDecimalSortOrder(t *testing.T) {
	t.Parallel()

	// Strictly increasing numeric values.
	ordered := []string{"-1000", "-12.5", "-1", "0", "0.5", "1", "20", "30", "1000000"}

	prev := encodeLex(t, ordered[0])

	for _, v := range ordered[1:] {
		cur := encodeLex(t, v)
		if bytes.Compare(prev, cur) >= 0 {
			t.Fatalf("sort order broken at %q: % x not < % x", v, prev, cur)
		}

		prev = cur
	}
}

func encodeLex(t *testing.T, n string) []byte {
	t.Helper()

	b, err := dataplane.LexDecimalEncodeForTest(n)
	if err != nil {
		t.Fatalf("LexDecimalEncodeForTest(%q): %v", n, err)
	}

	return b
}

// numericEqual compares two DynamoDB number literals by value, tolerating
// exponent-form vs plain-form differences (e.g. "12.5" vs "125E-1").
func numericEqual(a, b string) bool {
	na, oka := dataplane.DecimalNormalizeForTest(a)
	nb, okb := dataplane.DecimalNormalizeForTest(b)

	if !oka || !okb {
		return a == b
	}

	return na == nb || a == b
}
