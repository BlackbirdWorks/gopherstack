package cosmosdb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

// TestTriAnd_TruthTable exercises every cell of SQL 3VL's AND truth table:
// false is "stronger" than undefined (false wins over undefined), true
// requires both sides true, and undefined only surfaces when neither side
// is false and at least one side isn't a definite true.
func TestTriAnd_TruthTable(t *testing.T) {
	t.Parallel()

	u, f, tr := cosmosdb.TriUndefined, cosmosdb.TriFalse, cosmosdb.TriTrue

	tests := []struct {
		name string
		a, b cosmosdb.TriState
		want cosmosdb.TriState
	}{
		{name: "true and true", a: tr, b: tr, want: tr},
		{name: "true and false", a: tr, b: f, want: f},
		{name: "true and undefined", a: tr, b: u, want: u},
		{name: "false and true", a: f, b: tr, want: f},
		{name: "false and false", a: f, b: f, want: f},
		{name: "false and undefined", a: f, b: u, want: f},
		{name: "undefined and true", a: u, b: tr, want: u},
		{name: "undefined and false", a: u, b: f, want: f},
		{name: "undefined and undefined", a: u, b: u, want: u},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, cosmosdb.TriAnd(tt.a, tt.b))
		})
	}
}

// TestTriOr_TruthTable exercises every cell of SQL 3VL's OR truth table:
// true is "stronger" than undefined, false requires both sides false, and
// undefined only surfaces when neither side is true and at least one side
// isn't a definite false.
func TestTriOr_TruthTable(t *testing.T) {
	t.Parallel()

	u, f, tr := cosmosdb.TriUndefined, cosmosdb.TriFalse, cosmosdb.TriTrue

	tests := []struct {
		name string
		a, b cosmosdb.TriState
		want cosmosdb.TriState
	}{
		{name: "true or true", a: tr, b: tr, want: tr},
		{name: "true or false", a: tr, b: f, want: tr},
		{name: "true or undefined", a: tr, b: u, want: tr},
		{name: "false or true", a: f, b: tr, want: tr},
		{name: "false or false", a: f, b: f, want: f},
		{name: "false or undefined", a: f, b: u, want: u},
		{name: "undefined or true", a: u, b: tr, want: tr},
		{name: "undefined or false", a: u, b: f, want: u},
		{name: "undefined or undefined", a: u, b: u, want: u},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, cosmosdb.TriOr(tt.a, tt.b))
		})
	}
}

// TestTriNot_TruthTable is the crux of the three-valued-logic fix: NOT must
// invert true/false as usual, but undefined must stay undefined -- it must
// never flip to true, or "NOT (c.missing = 1)" would incorrectly match
// every document missing that field.
func TestTriNot_TruthTable(t *testing.T) {
	t.Parallel()

	u, f, tr := cosmosdb.TriUndefined, cosmosdb.TriFalse, cosmosdb.TriTrue

	tests := []struct {
		name string
		a    cosmosdb.TriState
		want cosmosdb.TriState
	}{
		{name: "not true", a: tr, want: f},
		{name: "not false", a: f, want: tr},
		{name: "not undefined stays undefined, never flips to true", a: u, want: u},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, cosmosdb.TriNot(tt.a))
		})
	}
}
