package gcpbigquery_test

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery"
)

func TestJobDroppedByResetOrRestoreDoesNotRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		drop func(t *testing.T, b *gcpbigquery.InMemoryBackend)
		name string
	}{
		{name: "reset", drop: func(_ *testing.T, b *gcpbigquery.InMemoryBackend) { b.Reset() }},
		{name: "restore", drop: func(t *testing.T, b *gcpbigquery.InMemoryBackend) {
			t.Helper()

			empty := gcpbigquery.NewInMemoryBackend()
			defer empty.Close()

			require.NoError(t, b.Restore(t.Context(), empty.Snapshot(t.Context())))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := gcpbigquery.NewInMemoryBackend()
				defer b.Close()

				g := newGate("RUNNING")
				b.SetJobStartHook(g.hook)

				insertQuery(t, b, queryJobBody("CREATE SCHEMA `p.ghost`", nil))
				<-g.reached

				tt.drop(t, b)
				close(g.release)
				synctest.Wait()

				_, err := b.GetDataset("p", "ghost")
				require.Error(t, err, "a job dropped by Reset or Restore must not touch the new state")
			})
		})
	}
}

func TestInsertJobAfterClose(t *testing.T) {
	t.Parallel()

	b := gcpbigquery.NewInMemoryBackend()
	b.Close()
	b.Close()

	_, err := b.InsertJob(t.Context(), "p", queryJobBody("SELECT 1", nil), nil)
	require.Error(t, err)
}

func TestRunQueryRequestIDReplay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sql      string
		hold     bool
		wantDone bool
		wantErr  bool
	}{
		{name: "replay while running", sql: "SELECT 1 AS x", hold: true},
		{name: "replay after success", sql: "SELECT 1 AS x", wantDone: true},
		{name: "replay after failure", sql: "SELECT 1 / 0 AS x", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := gcpbigquery.NewInMemoryBackend()
			t.Cleanup(b.Close)

			g := newGate("PENDING")
			b.SetJobStartHook(func(ctx context.Context, phase, project, id string) {
				if tt.hold {
					g.hook(ctx, phase, project, id)
				}
			})

			zero := 0
			req := map[string]any{"query": tt.sql, "useLegacySql": false, "requestId": "r1", "timeoutMs": zero}

			if !tt.hold {
				delete(req, "timeoutMs")
			}

			first, err := b.RunQuery(t.Context(), "p", req)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantDone, first["jobComplete"])
			}

			if tt.hold {
				<-g.reached
			}

			req["timeoutMs"] = zero
			again, err := b.RunQuery(t.Context(), "p", req)

			if tt.wantErr {
				require.Error(t, err, "a replayed failed query reports the failure")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantDone, again["jobComplete"])

			if tt.hold {
				close(g.release)
			}
		})
	}
}
