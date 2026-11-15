package container_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockercontainer "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

type statsAPI struct {
	body string
	mockAPI
}

func (s *statsAPI) ContainerStats(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.body)), nil
}

func TestDockerRuntime_Stats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		api     container.APIClient
		wantErr error
		name    string
		want    container.Stats
	}{
		{
			name: "cgroup v2 drops inactive file cache",
			api: &statsAPI{body: `{"cpu_stats":{"cpu_usage":{"total_usage":1500000}},` +
				`"memory_stats":{"usage":9000,"stats":{"inactive_file":4000}}}`},
			want: container.Stats{CPUTimeNanos: 1500000, MemoryBytes: 5000},
		},
		{
			name: "reported peak",
			api: &statsAPI{body: `{"cpu_stats":{"cpu_usage":{"total_usage":7}},` +
				`"memory_stats":{"usage":100,"max_usage":300}}`},
			want: container.Stats{CPUTimeNanos: 7, MemoryBytes: 100, MemoryPeakBytes: 300},
		},
		{name: "unsupported client", api: &mockAPI{}, wantErr: container.ErrStatsUnsupported},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := newRuntime(tt.api).Stats(t.Context(), "ctr-1")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

type graceAPI struct {
	gotTimeout *int
	mockAPI
}

func (g *graceAPI) ContainerStop(_ context.Context, _ string, opts dockercontainer.StopOptions) error {
	g.gotTimeout = opts.Timeout

	return nil
}

func TestDockerRuntime_StopAndRemoveWithin(t *testing.T) {
	t.Parallel()

	api := &graceAPI{}

	killed, err := newRuntime(api).StopAndRemoveWithin(t.Context(), "ctr-1", time.Second)
	require.NoError(t, err)
	assert.False(t, killed, "an immediate stop is not a kill")
	require.NotNil(t, api.gotTimeout)
	assert.Equal(t, 1, *api.gotTimeout, "the SIGTERM grace is passed to the stop call")
}
