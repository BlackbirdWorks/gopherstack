package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func TestAccessAnalyzerFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      any
		name    string
		wantEq  []string
		wantLen int
		exists  bool
		hasEx   bool
	}{
		{
			name:    "eq",
			in:      map[string]any{"principal.AWS": map[string]any{"Eq": []any{"1"}}},
			wantLen: 1,
			wantEq:  []string{"1"},
		},
		{
			name:    "exists",
			in:      map[string]any{"isPublic": map[string]any{"Exists": true}},
			wantLen: 1,
			hasEx:   true,
			exists:  true,
		},
		{name: "list_form_ignored", in: []any{map[string]any{"Property": "x"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := cloudformation.AccessAnalyzerFilterForTest(tt.in)
			require.Len(t, got, tt.wantLen)

			for _, c := range got {
				assert.Equal(t, tt.wantEq, c.Eq)

				if tt.hasEx {
					require.NotNil(t, c.Exists)
					assert.Equal(t, tt.exists, *c.Exists)
				}
			}
		})
	}
}
