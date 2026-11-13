package timestreamquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanonicalUnnestTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mod     string
		want    string
		wantErr bool
	}{
		{"round trip", unnestModuleFor([]string{"a", "b"}), unnestModuleFor([]string{"a", "b"}), false},
		{"empty payload", unnestTablePfx, unnestTablePfx, false},
		{"odd length", unnestTablePfx + "abc", "", true},
		{"not hex", unnestTablePfx + "zz", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := canonicalUnnestTable(tt.mod)
			if tt.wantErr {
				require.ErrorIs(t, err, errUnnestName)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
