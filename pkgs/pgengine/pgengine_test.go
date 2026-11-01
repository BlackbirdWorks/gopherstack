package pgengine_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/pgengine"
)

func TestQuoting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"ident plain", pgengine.Ident("app"), `"app"`},
		{"ident quote", pgengine.Ident(`a"b`), `"a""b"`},
		{"literal plain", pgengine.Literal("pw"), `'pw'`},
		{"literal quote", pgengine.Literal("it's"), `'it''s'`},
		{"literal backslash", pgengine.Literal(`a\b`), `'a\b'`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.got)
		})
	}
}

func TestNewRequiresRuntime(t *testing.T) {
	t.Parallel()

	_, err := pgengine.New(pgengine.Config{})
	require.ErrorIs(t, err, pgengine.ErrRuntimeUnavailable)
}
