package app

import (
	"log/slog"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

func TestFixedServicePorts_AllReported(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rangeStart int
		rangeEnd   int
	}{
		{"default pool", 10000, 10100},
		{"pool containing fixed ports", 4000, 9100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var root rootCLI

			parser, err := kong.New(&root, kong.Name("gopherstack"), kong.Writers(nil, nil))
			require.NoError(t, err)
			_, err = parser.Parse(nil)
			require.NoError(t, err)

			cli := root.Serve

			alloc, err := portalloc.New(tt.rangeStart, tt.rangeEnd)
			require.NoError(t, err)

			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			table := fixedServicePorts(cli)
			require.GreaterOrEqual(t, len(table), 21)

			reserved := alloc.Reserved()
			allocated := alloc.Allocated()

			for _, f := range table {
				if f.port <= 0 {
					continue
				}

				assert.Equal(t, f.key, reserved[f.port], f.label)
				assert.Equal(t, f.key, allocated[f.port], f.label)
			}
		})
	}
}
