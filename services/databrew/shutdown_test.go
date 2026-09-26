package databrew_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/databrew"
)

// TestBackendShutdown exercises the lifecycle context that bounds delayed job
// run transition goroutines, ensuring Shutdown cancels in-flight work and
// returns within the supplied context deadline.
func TestBackendShutdown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		build           func(t *testing.T) (*databrew.InMemoryBackend, string)
		name            string
		expectSucceeded bool
	}{
		{
			name: "shutdown before transition leaves run pending",
			build: func(t *testing.T) (*databrew.InMemoryBackend, string) {
				t.Helper()
				b := databrew.NewInMemoryBackendWithContext(
					t.Context(),
					"123456789012",
					"us-east-1",
				)
				_, err := b.CreateDataset(
					context.Background(),
					"ds",
					"CSV",
					s3Input("b", ""),
					databrew.DatasetFormatOptions{},
					nil,
					nil,
				)
				require.NoError(t, err)
				_, err = b.CreateJob(
					context.Background(),
					"sd-job",
					"PROFILE",
					"ds",
					"",
					"",
					"",
					nil,
					nil,
					databrew.JobExtras{},
				)
				require.NoError(t, err)
				_, err = b.StartJobRun(context.Background(), "sd-job")
				require.NoError(t, err)
				// Cancel immediately so the 100ms transition never fires.
				b.Shutdown(t.Context())

				return b, "sd-job"
			},
			expectSucceeded: false,
		},
		{
			name: "shutdown with no in-flight runs is a no-op",
			build: func(t *testing.T) (*databrew.InMemoryBackend, string) {
				t.Helper()
				b := databrew.NewInMemoryBackendWithContext(
					t.Context(),
					"123456789012",
					"us-east-1",
				)
				b.Shutdown(t.Context())

				return b, ""
			},
			expectSucceeded: false,
		},
		{
			name: "shutdown respects bounded context",
			build: func(t *testing.T) (*databrew.InMemoryBackend, string) {
				t.Helper()
				b := databrew.NewInMemoryBackendWithContext(
					t.Context(),
					"123456789012",
					"us-east-1",
				)
				_, err := b.CreateDataset(
					context.Background(),
					"ds",
					"CSV",
					s3Input("b", ""),
					databrew.DatasetFormatOptions{},
					nil,
					nil,
				)
				require.NoError(t, err)
				_, err = b.CreateJob(
					context.Background(),
					"sd-job2",
					"PROFILE",
					"ds",
					"",
					"",
					"",
					nil,
					nil,
					databrew.JobExtras{},
				)
				require.NoError(t, err)
				_, err = b.StartJobRun(context.Background(), "sd-job2")
				require.NoError(t, err)

				// An already-cancelled ctx must not block Shutdown.
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				done := make(chan struct{})
				go func() {
					b.Shutdown(ctx)
					close(done)
				}()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Fatal("Shutdown did not return within bound")
				}

				return b, "sd-job2"
			},
			expectSucceeded: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b, job := tt.build(t)
				if job == "" {
					return
				}

				// Cross the 100ms transition delay: a leaked goroutine would
				// have fired by now.
				time.Sleep(250 * time.Millisecond)
				synctest.Wait()

				runs, _, err := b.ListJobRuns(context.Background(), job, 100, "")
				require.NoError(t, err)
				require.Len(t, runs, 1)
				assert.NotEqual(t, "SUCCEEDED", runs[0].State)
			})
		})
	}
}

// TestResetDoesNotStopTransitions verifies Reset does not cancel the lifecycle
// context: a job run started after Reset must still transition to SUCCEEDED.
func TestResetDoesNotStopTransitions(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := databrew.NewInMemoryBackendWithContext(t.Context(), "123456789012", "us-east-1")
		b.Reset()

		_, err := b.CreateDataset(
			context.Background(),
			"ds",
			"CSV",
			s3Input("b", ""),
			databrew.DatasetFormatOptions{},
			nil,
			nil,
		)
		require.NoError(t, err)
		_, err = b.CreateJob(
			context.Background(),
			"post-reset",
			"PROFILE",
			"ds",
			"",
			"",
			"",
			nil,
			nil,
			databrew.JobExtras{},
		)
		require.NoError(t, err)
		_, err = b.StartJobRun(context.Background(), "post-reset")
		require.NoError(t, err)

		time.Sleep(200 * time.Millisecond)
		synctest.Wait()

		runs, _, err := b.ListJobRuns(context.Background(), "post-reset", 100, "")
		require.NoError(t, err)
		require.Len(t, runs, 1)
		assert.Equal(t, "SUCCEEDED", runs[0].State)
	})
}
