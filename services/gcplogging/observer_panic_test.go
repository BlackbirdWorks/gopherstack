package gcplogging_test

import (
	"sync/atomic"
	"testing"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ltype "google.golang.org/genproto/googleapis/logging/type"
)

func TestObserverPanicIsolated(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)

	var seen atomic.Int32

	e.handler.Backend.ObserveEntries(func(*loggingpb.LogEntry) { panic("observer bug") })
	e.handler.Backend.ObserveEntries(func(*loggingpb.LogEntry) { seen.Add(1) })

	e.write(t, textEntry("app", "one", ltype.LogSeverity_INFO, e.clk.now()),
		textEntry("app", "two", ltype.LogSeverity_INFO, e.clk.now()))

	require.Equal(t, int32(2), seen.Load())
	assert.Len(t, e.list(t, &loggingpb.ListLogEntriesRequest{}), 2)
}
