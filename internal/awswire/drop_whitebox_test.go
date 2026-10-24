package awswire

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDropMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in    any
		want  any
		name  string
		names []string
	}{
		{
			name:  "nested",
			names: []string{"U"},
			in: map[string]any{
				"A": 1.0, "U": map[string]any{"x": 1.0},
				"L": []any{map[string]any{"U": "gone", "K": "kept"}},
			},
			want: map[string]any{"A": 1.0, "L": []any{map[string]any{"K": "kept"}}},
		},
		{name: "no_names", in: map[string]any{"U": 1.0}, want: map[string]any{"U": 1.0}},
		{name: "scalar", names: []string{"U"}, in: "s", want: "s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, dropMembers(tt.in, stripSet(tt.names)))
		})
	}
}
