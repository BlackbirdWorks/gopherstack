package gcpbigquery_test

import (
	"context"
	"encoding/json/v2"
	"maps"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery"
)

type gate struct {
	reached chan struct{}
	release chan struct{}
	phase   string
}

func newGate(phase string) *gate {
	return &gate{phase: phase, reached: make(chan struct{}, 1), release: make(chan struct{})}
}

func (g *gate) hook(ctx context.Context, phase, _, _ string) {
	if phase != g.phase {
		return
	}

	g.reached <- struct{}{}

	select {
	case <-g.release:
	case <-ctx.Done():
	}
}

func queryJobBody(sql string, extra map[string]any) map[string]any {
	cfg := map[string]any{"query": map[string]any{"query": sql, "useLegacySql": false}}
	maps.Copy(cfg, extra)

	return map[string]any{"configuration": cfg}
}

func jobStatus(t *testing.T, b *gcpbigquery.InMemoryBackend, id string) map[string]any {
	t.Helper()

	job, err := b.GetJob("p", id)
	require.NoError(t, err)

	raw, err := json.Marshal(job["status"], jsonx.V1())
	require.NoError(t, err)

	var status map[string]any
	require.NoError(t, json.Unmarshal(raw, &status, jsonx.V1()))

	return status
}

func insertQuery(t *testing.T, b *gcpbigquery.InMemoryBackend, body map[string]any) string {
	t.Helper()

	out, err := b.InsertJob(t.Context(), "p", body, nil)
	require.NoError(t, err)

	ref, ok := out["jobReference"].(map[string]any)
	require.True(t, ok)

	raw, err := json.Marshal(out["status"], jsonx.V1())
	require.NoError(t, err)
	assert.Contains(t, string(raw), "PENDING", "jobs.insert answers before the job ran")

	id, _ := ref["jobId"].(string)

	return id
}

func TestAsyncJobStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		phase     string
		wantState string
		wantRes   string
		cancel    bool
	}{
		{name: "pending then done", phase: "PENDING", wantState: "PENDING"},
		{name: "running then done", phase: "RUNNING", wantState: "RUNNING"},
		{name: "cancel pending", phase: "PENDING", cancel: true, wantState: "PENDING", wantRes: "stopped"},
		{name: "cancel running", phase: "RUNNING", cancel: true, wantState: "RUNNING", wantRes: "stopped"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := gcpbigquery.NewInMemoryBackend()
			t.Cleanup(b.Close)

			g := newGate(tt.phase)
			b.SetJobStartHook(g.hook)

			id := insertQuery(t, b, queryJobBody("SELECT 1 AS x", nil))
			<-g.reached
			assert.Equal(t, tt.wantState, jobStatus(t, b, id)["state"])

			if tt.cancel {
				_, err := b.CancelJob("p", id)
				require.NoError(t, err)
			}

			close(g.release)

			timeout := 60000
			res, err := b.GetQueryResults("p", id, gcpbigquery.QueryResultOptions{TimeoutMs: &timeout})

			status := jobStatus(t, b, id)
			assert.Equal(t, "DONE", status["state"])

			if tt.wantRes != "" {
				require.Error(t, err)

				errRes, _ := status["errorResult"].(map[string]any)
				assert.Equal(t, tt.wantRes, errRes["reason"])

				return
			}

			require.NoError(t, err)
			assert.Equal(t, true, res["jobComplete"])
		})
	}
}

func TestAsyncWaitAndTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extra   map[string]any
		hold    func(ctx context.Context) // runs in the PENDING hook
		name    string
		wantErr string
		poll    int
		wantIn  bool
	}{
		{name: "long poll expires", poll: 5000, hold: func(ctx context.Context) { <-ctx.Done() }, wantIn: true},
		{
			name: "job timeout", extra: map[string]any{"jobTimeoutMs": "100"}, poll: 600000,
			hold: func(ctx context.Context) { <-ctx.Done() }, wantErr: "timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := gcpbigquery.NewInMemoryBackend()
				defer b.Close()

				b.SetJobStartHook(func(ctx context.Context, phase, _, _ string) {
					if phase == "PENDING" {
						tt.hold(ctx)
					}
				})

				id := insertQuery(t, b, queryJobBody("SELECT 1", tt.extra))

				start := time.Now()
				res, err := b.GetQueryResults("p", id, gcpbigquery.QueryResultOptions{TimeoutMs: &tt.poll})

				if tt.wantIn {
					require.NoError(t, err)
					assert.Equal(t, false, res["jobComplete"])
					assert.Equal(t, 5*time.Second, time.Since(start))

					return
				}

				require.Error(t, err)
				errRes, _ := jobStatus(t, b, id)["errorResult"].(map[string]any)
				assert.Equal(t, tt.wantErr, errRes["reason"])
			})
		})
	}
}

func TestRunQueryTimeoutMs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		timeout  any
		name     string
		complete bool
	}{
		{0, "zero returns at once", false},
		{nil, "default waits", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := gcpbigquery.NewInMemoryBackend()
			t.Cleanup(b.Close)

			g := newGate("PENDING")
			b.SetJobStartHook(g.hook)

			req := map[string]any{"query": "SELECT 1 AS x", "useLegacySql": false}
			if tt.timeout != nil {
				req["timeoutMs"] = tt.timeout
			}

			if tt.complete {
				close(g.release)
			}

			resp, err := b.RunQuery(t.Context(), "p", req)
			require.NoError(t, err)
			assert.Equal(t, tt.complete, resp["jobComplete"])

			if !tt.complete {
				close(g.release)
			}
		})
	}
}

func TestAsyncJobSnapshotInterrupted(t *testing.T) {
	t.Parallel()

	b := gcpbigquery.NewInMemoryBackend()
	g := newGate("PENDING")
	b.SetJobStartHook(g.hook)

	id := insertQuery(t, b, queryJobBody("SELECT 1", nil))
	<-g.reached

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	close(g.release)
	b.Close()

	fresh := gcpbigquery.NewInMemoryBackend()
	t.Cleanup(fresh.Close)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	status := jobStatus(t, fresh, id)
	assert.Equal(t, "DONE", status["state"])

	errRes, _ := status["errorResult"].(map[string]any)
	assert.Equal(t, "stopped", errRes["reason"])
}
