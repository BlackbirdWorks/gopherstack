package dax_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// ---- DescribeEvents ----

func TestDescribeEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(b *dax.InMemoryBackend)
		sourceName string
		sourceType string
		wantMin    int
	}{
		{
			name: "events emitted on create",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("evt-cluster"))
			},
			sourceType: dax.EventSourceTypeCluster,
			wantMin:    1,
		},
		{
			name: "filter by source name",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("target-cluster"))
				_, _ = b.CreateCluster(validCreateInput("other-cluster"))
			},
			sourceName: "target-cluster",
			wantMin:    1,
		},
		{
			name:    "no events on fresh backend",
			setup:   func(_ *dax.InMemoryBackend) {},
			wantMin: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			events, _, err := b.DescribeEvents(tt.sourceName, tt.sourceType, nil, nil, 0, "")
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(events), tt.wantMin)

			if tt.sourceName != "" {
				for _, ev := range events {
					assert.Equal(t, tt.sourceName, ev.SourceName)
				}
			}

			if tt.sourceType != "" {
				for _, ev := range events {
					assert.Equal(t, tt.sourceType, ev.SourceType)
				}
			}
		})
	}
}
