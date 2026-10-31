package inithooks_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/inithooks"
)

func TestRunNamedRejectsTraversalStage(t *testing.T) {
	t.Parallel()

	for _, stage := range []inithooks.Stage{"../../etc", "ready/../boot", "", "nope"} {
		t.Run(string(stage), func(t *testing.T) {
			t.Parallel()

			m := inithooks.NewManager(inithooks.ManagerConfig{Roots: []string{t.TempDir()}})
			_, err := m.RunNamed(t.Context(), stage, "")
			require.ErrorIs(t, err, inithooks.ErrUnknownStage)
			assert.Empty(t, m.Status())
		})
	}
}
