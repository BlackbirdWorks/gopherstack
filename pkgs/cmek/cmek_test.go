package cmek_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
)

type fakeChecker struct{ err error }

func (f fakeChecker) CheckKey(string, string) error { return f.err }

func TestLocation(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, key, want string }{
		{"key", "projects/p/locations/us-central1/keyRings/r/cryptoKeys/k", "us-central1"},
		{"short", "projects/p", ""},
		{"empty", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, cmek.Location(tt.key))
		})
	}
}

func TestCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		c    cmek.Checker
		want error
		name string
		key  string
	}{
		{name: "nil checker", c: nil, key: "k"},
		{name: "empty key", c: fakeChecker{err: cmek.ErrNotFound}, key: ""},
		{name: "missing", c: fakeChecker{err: cmek.ErrNotFound}, key: "k", want: cmek.ErrNotFound},
		{name: "ok", c: fakeChecker{}, key: "k"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := cmek.Check(tt.c, tt.key, "")
			if tt.want == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, tt.want)
		})
	}
}
