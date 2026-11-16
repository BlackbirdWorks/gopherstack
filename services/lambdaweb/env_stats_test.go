package lambdaweb_test

import (
	"context"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

// statsEnv adds resource stats and a SIGTERM-grace outcome to an environment.
type statsEnv struct {
	lambdaweb.Environment
	stats  lambdaweb.EnvStats
	killed bool
}

func (e *statsEnv) Stats(context.Context) (lambdaweb.EnvStats, bool) { return e.stats, true }

func (e *statsEnv) CloseWithin(ctx context.Context, _ time.Duration) bool {
	e.Close(ctx)

	return !e.killed
}

type statsLauncher struct {
	inner  *fakeLauncher
	stats  lambdaweb.EnvStats
	killed bool
}

func (l *statsLauncher) Launch(ctx context.Context, spec lambdaweb.LaunchSpec) (lambdaweb.Environment, error) {
	env, err := l.inner.Launch(ctx, spec)
	if err != nil {
		return nil, err
	}

	return &statsEnv{Environment: env, stats: l.stats, killed: l.killed}, nil
}

func newStatsEnv(t *testing.T, killed bool) (*dataEnv, *fakeLogs, *fakeMetrics) {
	t.Helper()

	d, logs := newLoggedEnv(t, logOpts{})
	m := &fakeMetrics{}
	d.backend.SetMetricEmitter(m)
	d.backend.SetLauncher(&statsLauncher{
		inner: d.launcher,
		stats: lambdaweb.EnvStats{
			CPUTime: 1500 * time.Millisecond, MemoryBytes: 100 << 20, PeakMemoryBytes: 150 << 20,
		},
		killed: killed,
	})

	return d, logs, m
}

func recordOf(t *testing.T, evs []map[string]any, typ string) map[string]any {
	t.Helper()

	for _, ev := range evs {
		if ev["type"] == typ {
			rec, ok := ev["record"].(map[string]any)
			require.True(t, ok)

			return rec
		}
	}

	require.FailNow(t, "no event", typ)

	return nil
}

func TestEnvironmentStats(t *testing.T) {
	t.Parallel()

	d, logs, m := newStatsEnv(t, false)
	require.Equal(t, http.StatusOK, d.get(t).StatusCode)

	d.clock.Advance(61 * time.Second)
	d.backend.SweepEnvironments()

	require.Eventually(t, func() bool {
		return m.find(nsWeb, "EnvironmentCPUTime") != nil
	}, 5*time.Second, 5*time.Millisecond)

	assert.InDelta(t, 1500, m.sum(nsWeb, "EnvironmentCPUTime"), 0)
	assert.InDelta(t, 150, m.find(nsWeb, "EnvironmentMaxMemoryUsage").Value, 0)
	assert.Equal(t, "Megabytes", m.find(nsWeb, "EnvironmentMaxMemoryUsage").Unit)

	evs := events(t, logs, "/aws/lambda/web/app")

	initMetrics := recordOf(t, evs, "platform.initReport")["metrics"].(map[string]any)
	assert.InDelta(t, 1500, initMetrics["totalCpuTimeMs"], 0)
	assert.InDelta(t, 150, initMetrics["maxMemoryUsageMb"], 0)

	summary := recordOf(t, evs, "platform.invokeSummary")["metrics"].(map[string]any)
	assert.InDelta(t, 1500, summary["cpuTimeMs"], 0)
	assert.InDelta(t, 150, summary["maxMemoryUsageMb"], 0)
}

func TestSpinDownReport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		wantLevel  string
		wantErr    string
		killed     bool
	}{
		{name: "exits on sigterm", wantStatus: "success", wantLevel: "INFO"},
		{name: "killed after grace", killed: true, wantStatus: "failure", wantLevel: "WARN",
			wantErr: "Runtime.SpinDownTimeout"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d, logs, _ := newStatsEnv(t, tc.killed)
			require.Equal(t, http.StatusOK, d.get(t).StatusCode)

			d.clock.Advance(6 * time.Minute)
			d.backend.SweepEnvironments()

			require.Eventually(t, func() bool {
				return len(eventTypes(events(t, logs, "/aws/lambda/web/app"))) > 0 &&
					slices.Contains(eventTypes(events(t, logs, "/aws/lambda/web/app")), "platform.spinDownReport")
			}, 5*time.Second, 5*time.Millisecond)

			evs := events(t, logs, "/aws/lambda/web/app")
			rec := recordOf(t, evs, "platform.spinDownReport")
			assert.Equal(t, tc.wantStatus, rec["status"])
			assert.Equal(t, tc.wantErr, stringOf(rec["errorType"]))

			for _, ev := range evs {
				if ev["type"] == "platform.spinDownReport" {
					assert.Equal(t, tc.wantLevel, ev["level"])
				}
			}

			inv := recordOf(t, evs, "platform.invokeReport")["metrics"].(map[string]any)
			assert.InDelta(t, 1500, inv["totalCpuTimeMs"], 0)
		})
	}
}

func stringOf(v any) string {
	s, _ := v.(string)

	return s
}
