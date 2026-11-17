package gcpcloudbuild_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
)

func TestTriggerFilterCEL(t *testing.T) {
	t.Parallel()

	vars := map[string]string{"_ENV": "prod", "_TAG": "v1.2", "_EMPTY": ""}

	tests := []struct {
		name    string
		filter  string
		want    bool
		wantErr bool
	}{
		{name: "equals", filter: `_ENV == "prod"`, want: true},
		{name: "size", filter: `_TAG.size() == 4`, want: true},
		{name: "membership", filter: `_ENV in ["dev", "prod"]`, want: true},
		{name: "ternary", filter: `_ENV == "prod" ? _TAG.startsWith("v") : false`, want: true},
		{name: "regex", filter: `_TAG.matches("^v[0-9]+[.][0-9]+$")`, want: true},
		{name: "concat", filter: `_ENV + "-" + _TAG == "prod-v1.2"`, want: true},
		{name: "negative", filter: `!(_ENV == "prod")`, want: false},
		{name: "syntax", filter: `_ENV ==`, wantErr: true},
		{name: "unbound", filter: `_NOPE == "x"`, wantErr: true},
		{name: "not bool", filter: `_ENV`, wantErr: true},
		{name: "type mismatch", filter: `_ENV == 1`, wantErr: true},
		{name: "too long", filter: `_ENV == "` + strings.Repeat("a", 5000) + `"`, wantErr: true},
		{name: "deep", filter: strings.Repeat("(", 200) + "true" + strings.Repeat(")", 200), wantErr: true},
		{name: "long literal", filter: `"` + strings.Repeat("a", 3000) + `".matches("(a|b)*c")`, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := gcpcloudbuild.EvalTriggerFilter(tt.filter, vars)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
