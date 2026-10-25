package wfinterp_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

// fakeClock sleeps without waiting: it advances virtual time and records every sleep.
type fakeClock struct {
	now    time.Time
	sleeps []time.Duration
	mu     sync.Mutex
}

func newClock() *fakeClock { return &fakeClock{now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.sleeps = append(c.sleeps, d)
	c.now = c.now.Add(d)

	return nil
}

func (c *fakeClock) slept() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]time.Duration(nil), c.sleeps...)
}

type logSink struct {
	entries []wfinterp.LogEntry
	mu      sync.Mutex
}

func (l *logSink) add(e wfinterp.LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.entries = append(l.entries, e)
}

type result struct {
	value any
	err   error
	clock *fakeClock
	logs  *logSink
}

func run(t *testing.T, src string, arg any, mutate func(*wfinterp.Host)) result {
	t.Helper()

	eng := wfinterp.NewEngine()
	prog, err := eng.Compile(src)
	require.NoError(t, err)

	r := result{clock: newClock(), logs: &logSink{}}
	host := &wfinterp.Host{
		Clock: r.clock,
		Log:   r.logs.add,
		Env: func(n string) (string, bool) {
			return map[string]string{"GOOGLE_CLOUD_PROJECT_ID": "p1"}[n], n == "GOOGLE_CLOUD_PROJECT_ID"
		},
	}

	if mutate != nil {
		mutate(host)
	}

	r.value, r.err = eng.Run(t.Context(), prog, arg, host)

	return r
}

func sprintf(format string, a ...any) string { return fmt.Sprintf(format, a...) }

func replaceAll(s, old, repl string) string { return strings.ReplaceAll(s, old, repl) }
