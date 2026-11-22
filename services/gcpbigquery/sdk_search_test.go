package gcpbigquery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDKSearchLogicalPhrase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{"phrase and or", `SELECT SEARCH(R'foo bar', R'"foo AND (bar OR baz)"')`, true},
		{"plain and or", `SELECT SEARCH(R'foo', R'foo AND (bar OR baz)')`, false},
		{"phrase adjacent", `SELECT SEARCH(R'foo bar baz', R'"foo bar"')`, true},
	}

	env := newSDK(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows := env.rows(t, tt.sql)
			require.Len(t, rows, 1)
			assert.Equal(t, tt.want, rows[0][0])
		})
	}
}
