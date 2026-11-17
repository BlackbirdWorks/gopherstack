package mysqlengine_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/mysqlengine"
)

func TestSetVariableRefusesUnsafeNames(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"wait_timeout; DROP DATABASE x", "Wait_Timeout", "", "a b", "max-connections", "x`y"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := (&mysqlengine.Server{}).SetVariable(t.Context(), name, "1", false, false)
			require.ErrorIs(t, err, mysqlengine.ErrInvalidVariable)
		})
	}
}

func TestDumpToRefusesUnsafeDatabaseNames(t *testing.T) {
	t.Parallel()

	for _, db := range []string{"-h", "a b", "a;b", "a`b", ""} {
		t.Run(db, func(t *testing.T) {
			t.Parallel()

			err := (&mysqlengine.Server{}).DumpTo(t.Context(), []string{db}, &bytes.Buffer{})
			require.ErrorIs(t, err, mysqlengine.ErrInvalidIdentifier)
		})
	}
}
