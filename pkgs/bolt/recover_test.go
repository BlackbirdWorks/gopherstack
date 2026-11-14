package bolt_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/bolt"
)

func TestBackendPanicAnswersFailureAndKeepsServing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		ver   bolt.Version
	}{
		{name: "v5.4", query: "PANIC", ver: bolt.Version{Major: 5, Minor: 4}},
		{name: "v4.4", query: "PANIC", ver: bolt.Version{Major: 4, Minor: 4}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := &fakeBackend{rows: rowsOf(2)}

			bad := startServer(t, b, tt.ver)
			login(t, bad)

			other := startServer(t, b, tt.ver)
			login(t, other)

			r, err := bad.Call(bolt.TagRun, tt.query, map[string]any{}, map[string]any{})
			require.NoError(t, err)
			require.Equal(t, bolt.TagFailure, r.Tag)
			assert.Equal(t, bolt.CodeDatabaseError, r.Meta()["code"])

			r, err = bad.Call(bolt.TagReset)
			require.NoError(t, err)
			require.Equal(t, bolt.TagSuccess, r.Tag)

			for _, c := range []*bolt.Client{bad, other} {
				rows, _, qerr := c.Query("MATCH (n) RETURN n", nil)
				require.NoError(t, qerr)
				assert.Len(t, rows, 2)
			}
		})
	}
}
