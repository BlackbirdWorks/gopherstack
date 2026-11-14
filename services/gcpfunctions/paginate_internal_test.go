package gcpfunctions

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPaginateSizeBounds(t *testing.T) {
	t.Parallel()

	all := make([]int, 1500)

	tests := []struct {
		name string
		size int32
		want int
	}{
		{name: "omitted", size: 0, want: 1000},
		{name: "above max", size: 5000, want: 1000},
		{name: "explicit", size: 7, want: 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, next, err := paginate(all, "", tt.size)
			require.NoError(t, err)
			assert.Len(t, got, tt.want)
			assert.NotEmpty(t, next)
		})
	}
}
