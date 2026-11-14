package athena

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseClauseIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want int
	}{
		{"zero", "0", 0},
		{"small", "3", 3},
		{"negative", "-2", -2},
		{"not a number", "x", 0},
		{"above int32", "99999999999", 2147483647},
		{"below int32", "-99999999999", -2147483648},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, parseClauseIndex(tt.in))
		})
	}
}
