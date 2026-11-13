package appsync_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestRenderVTL_ListIndexBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tmpl string
		want string
	}{
		{name: "in_range", tmpl: `#set($l=["a","b"])$l.remove(1)|$l.size()`, want: "b|1"},
		{
			name: "beyond_int32",
			tmpl: `#set($l=["a","b"])$l.remove(5000000000)|$l.size()`,
			want: "$l.remove(5000000000)|2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := appsync.RenderVTL(tt.tmpl, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
