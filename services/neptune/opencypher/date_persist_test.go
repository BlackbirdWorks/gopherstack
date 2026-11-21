package opencypher_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func TestExportImportKeepsDateValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		val  any
		name string
	}{
		{time.UnixMilli(1_700_000_000_123).UTC(), "date"},
		{[]any{time.UnixMilli(5).UTC(), int64(7), 1.5}, "multi valued"},
		{time.UnixMilli(-86_400_000).UTC(), "before epoch"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := opencypher.NewGraph()
			require.NoError(t, g.Update(t.Context(), func(s *opencypher.Store) error {
				_, err := s.AddNode("n", []string{"D"}, map[string]any{"k": tt.val})

				return err
			}))

			dump, err := g.Export(t.Context())
			require.NoError(t, err)

			g2 := opencypher.NewGraph()
			require.NoError(t, g2.Import(t.Context(), dump))
			require.NoError(t, g2.View(t.Context(), func(s *opencypher.Store) error {
				assert.Equal(t, tt.val, s.Node("n").Props["k"])

				return nil
			}))
		})
	}
}
