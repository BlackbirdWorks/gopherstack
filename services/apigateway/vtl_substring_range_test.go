package apigateway_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

func TestVTL_SubstringIntRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tmpl string
		want string
	}{
		{name: "in_range", tmpl: `#set($s="hello")$s.substring(1, 3)`, want: "el"},
		{
			name: "end_above_int32",
			tmpl: `#set($s="hello")$s.substring(0, 99999999999)`,
			want: "$s.substring(0, 99999999999)",
		},
		{
			name: "start_above_int32",
			tmpl: `#set($s="hello")$s.substring(99999999999)`,
			want: "$s.substring(99999999999)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, apigateway.RenderTemplate(tt.tmpl, apigateway.VTLContext{}))
		})
	}
}
