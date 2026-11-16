package gcpcloudbuild_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
)

func TestTriggerFilterBounds(t *testing.T) {
	t.Parallel()

	list := "[" + strings.TrimSuffix(strings.Repeat(`"x",`, 300), ",") + "]"
	nested := list + `.all(a, ` + list + `.all(b, ` + list + `.all(c, a == b || c != "")))`
	concat := `_V` + strings.Repeat(` + _V`, 500) + ` != ""`

	tests := []struct {
		vars   map[string]string
		name   string
		filter string
	}{
		{name: "nested comprehension cost", filter: nested},
		{name: "large concatenation", filter: concat, vars: map[string]string{"_V": strings.Repeat("v", 200000)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := gcpcloudbuild.EvalTriggerFilter(tt.filter, tt.vars)
			require.Error(t, err)
		})
	}
}
