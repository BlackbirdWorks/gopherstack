package lambdaweb_test

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

var errSinkDown = errors.New("log group unavailable")

// flakySink fails PutLogLines while down is set.
type flakySink struct {
	inner *fakeLogs
	down  atomic.Bool
}

func (f *flakySink) EnsureLogGroupAndStream(ctx context.Context, region, group, stream string) error {
	return f.inner.EnsureLogGroupAndStream(ctx, region, group, stream)
}

func (f *flakySink) PutLogLines(ctx context.Context, region, group, stream string, lines []string) error {
	if f.down.Load() {
		return errSinkDown
	}

	return f.inner.PutLogLines(ctx, region, group, stream, lines)
}

func TestLogs_DroppedEventsAreReported(t *testing.T) {
	t.Parallel()

	d, logs := newLoggedEnv(t, logOpts{
		emit: func(_ lambdaweb.LaunchSpec, line func(bool, string)) { line(false, "hello") },
	})

	sink := &flakySink{inner: logs}
	sink.down.Store(true)
	d.backend.SetLogSink(sink)

	require.Equal(t, http.StatusOK, d.get(t).StatusCode)
	assert.Empty(t, logs.snapshot("/aws/lambda/web/app"), "nothing is delivered while the log group is unavailable")

	sink.down.Store(false)
	require.Equal(t, http.StatusOK, d.get(t).StatusCode)

	evs := events(t, logs, "/aws/lambda/web/app")
	require.Len(t, evs, 2)

	assert.Equal(t, "platform.logsDropped", evs[0]["type"])
	assert.Equal(t, "WARN", evs[0]["level"])

	rec, ok := evs[0]["record"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 4, rec["droppedRecords"], 0, "initStart, initReport, invokeStart and the first line")
	assert.Positive(t, rec["droppedBytes"])
	assert.NotEmpty(t, rec["reason"])
	assert.Equal(t, "hello", evs[1]["message"])

	require.Equal(t, http.StatusOK, d.get(t).StatusCode)

	again := events(t, logs, "/aws/lambda/web/app")
	require.Len(t, again, 3, "the counters reset after the warning is delivered")
	assert.Nil(t, again[2]["type"])
}
