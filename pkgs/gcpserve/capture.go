package gcpserve

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// LogCapture forwards workload output and request logs to a LogSink once one is wired.
type LogCapture struct {
	sink LogSink
	mu   *lockmetrics.RWMutex
	wg   sync.WaitGroup
}

// NewLogCapture returns a LogCapture whose lock is named lockName.
func NewLogCapture(lockName string) *LogCapture {
	return &LogCapture{mu: lockmetrics.New(lockName)}
}

// SetSink wires the destination; nil disables capture.
func (c *LogCapture) SetSink(s LogSink) {
	c.mu.Lock("SetSink")
	defer c.mu.Unlock()

	c.sink = s
}

func (c *LogCapture) current() LogSink {
	c.mu.RLock("current")
	defer c.mu.RUnlock()

	return c.sink
}

// Start follows container id's output in the background until the container is removed or ctx ends.
func (c *LogCapture) Start(ctx context.Context, rt any, id string, t LogTarget) {
	sink := c.current()
	if sink == nil {
		return
	}

	c.wg.Go(func() {
		if err := StreamLogs(ctx, rt, id, t, sink); err != nil {
			logger.Load(ctx).DebugContext(ctx, "gcpserve: log capture ended", "container", id, "error", err)
		}
	})
}

// Request writes the request log for a finished request; a no-op without a sink.
func (c *LogCapture) Request(
	ctx context.Context,
	t LogTarget,
	r *http.Request,
	rec *RequestRecorder,
	took time.Duration,
) {
	if sink := c.current(); sink != nil {
		t.LogRequest(ctx, sink, r, rec, took)
	}
}

// Wait blocks until every capture goroutine has finished; call after the containers are removed.
func (c *LogCapture) Wait() { c.wg.Wait() }
