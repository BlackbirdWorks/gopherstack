package mediastore_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/mediastore"
)

func TestInMemoryBackend_MetricPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errSentinel error
		setup       func(t *testing.T, b *mediastore.InMemoryBackend)
		name        string
		container   string
		policy      mediastore.MetricPolicy
		wantErr     bool
	}{
		{
			name:      "put and get metric policy",
			container: "metric-container",
			policy: mediastore.MetricPolicy{
				ContainerLevelMetrics: "ENABLED",
			},
		},
		{
			name:        "get metric policy on missing container",
			container:   "missing",
			wantErr:     true,
			errSentinel: awserr.ErrNotFound,
		},
		{
			name:      "get metric policy when none set",
			container: "no-metric",
			setup: func(t *testing.T, b *mediastore.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateContainer(context.Background(), testAccountID, "no-metric", nil)
				require.NoError(t, err)
			},
			wantErr:     true,
			errSentinel: awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			if tt.setup != nil {
				tt.setup(t, b)
			}

			if tt.wantErr {
				_, err := b.GetMetricPolicy(context.Background(), tt.container)
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errSentinel)

				return
			}

			_, err := b.CreateContainer(context.Background(), testAccountID, tt.container, nil)
			require.NoError(t, err)

			err = b.PutMetricPolicy(context.Background(), tt.container, tt.policy)
			require.NoError(t, err)

			got, err := b.GetMetricPolicy(context.Background(), tt.container)
			require.NoError(t, err)
			assert.Equal(t, tt.policy.ContainerLevelMetrics, got.ContainerLevelMetrics)

			err = b.DeleteMetricPolicy(context.Background(), tt.container)
			require.NoError(t, err)

			_, err = b.GetMetricPolicy(context.Background(), tt.container)
			require.Error(t, err)
		})
	}
}
