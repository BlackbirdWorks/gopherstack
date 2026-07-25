package strs_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

func TestFold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{"already lowercase", "mydb", "mydb"},
		{"mixed case", "MyDB", "mydb"},
		{"all uppercase", "MYDB", "mydb"},
		{"with hyphens", "My-DB-1", "my-db-1"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := strs.Fold(tt.in); got != tt.want {
				t.Errorf("Fold(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestFold_Collision(t *testing.T) {
	t.Parallel()

	// The whole point of Fold: two identifiers that only differ in case must
	// fold to the same key, so a store keyed by Fold's output collides them
	// the way real AWS does.
	if strs.Fold("MyDB") != strs.Fold("mydb") {
		t.Errorf("Fold(%q) and Fold(%q) should collide", "MyDB", "mydb")
	}
}

func TestEqual(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{"identical", "mydb", "mydb", true},
		{"different case", "MyDB", "mydb", true},
		{"different case both", "MYDB", "mydb", true},
		{"different identifiers", "mydb", "otherdb", false},
		{"both empty", "", "", true},
		{"one empty", "mydb", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := strs.Equal(tt.a, tt.b); got != tt.want {
				t.Errorf("Equal(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestContainsFold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		values []string
		want   bool
	}{
		{name: "exact match", values: []string{"mydb", "otherdb"}, target: "mydb", want: true},
		{name: "case-insensitive match", values: []string{"MyDB", "otherdb"}, target: "mydb", want: true},
		{name: "target upper, value lower", values: []string{"mydb"}, target: "MYDB", want: true},
		{name: "no match", values: []string{"otherdb", "thirddb"}, target: "mydb", want: false},
		{name: "empty values", values: []string{}, target: "mydb", want: false},
		{name: "nil values", values: nil, target: "mydb", want: false},
		{name: "empty target present", values: []string{"", "mydb"}, target: "", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := strs.ContainsFold(tt.values, tt.target); got != tt.want {
				t.Errorf("ContainsFold(%v, %q) = %v, want %v", tt.values, tt.target, got, tt.want)
			}
		})
	}
}
