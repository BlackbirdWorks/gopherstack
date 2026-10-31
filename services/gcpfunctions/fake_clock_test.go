package gcpfunctions_test

import (
	"sync"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
)

type fakeTimer struct {
	clock *fakeClock
	ch    chan time.Time
	fn    func()
	at    time.Time
	d     time.Duration
	armed bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()

	was := t.armed
	t.armed = false

	return was
}

// fakeClock is a manually advanced gcpscale.Clock that remembers every timer duration it was asked for.
type fakeClock struct {
	now     time.Time
	changed chan struct{}
	timers  []*fakeTimer
	asked   []time.Duration
	mu      sync.Mutex
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), changed: make(chan struct{}, 1024)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *fakeClock) add(d time.Duration, fn func()) *fakeTimer {
	c.mu.Lock()

	t := &fakeTimer{clock: c, ch: make(chan time.Time, 1), fn: fn, at: c.now.Add(d), d: d, armed: true}
	c.timers = append(c.timers, t)

	if fn == nil {
		c.asked = append(c.asked, d)
	}
	c.mu.Unlock()

	select {
	case c.changed <- struct{}{}:
	default:
	}

	return t
}

func (c *fakeClock) NewTimer(d time.Duration) gcpscale.Timer { return c.add(d, nil) }

func (c *fakeClock) AfterFunc(d time.Duration, f func()) gcpscale.Timer { return c.add(d, f) }

// pending counts armed timers; onlyNew restricts it to NewTimer timers (queue and retry waits, not idle timers).
func (c *fakeClock) pending(onlyNew bool) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := 0

	for _, t := range c.timers {
		if t.armed && (!onlyNew || t.fn == nil) {
			n++
		}
	}

	return n
}

// durations returns the duration of every NewTimer so far, in order (AfterFunc idle timers are not recorded).
func (c *fakeClock) durations() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]time.Duration(nil), c.asked...)
}

// waitTimers blocks until at least n timers are armed; waitNew counts only NewTimer timers.
func (c *fakeClock) waitTimers(t *testing.T, n int) { t.Helper(); c.wait(t, n, false) }

func (c *fakeClock) waitNew(t *testing.T) { t.Helper(); c.wait(t, 1, true) }

func (c *fakeClock) wait(t *testing.T, n int, onlyNew bool) {
	t.Helper()

	for c.pending(onlyNew) < n {
		select {
		case <-c.changed:
		case <-t.Context().Done():
			t.Fatalf("timed out waiting for %d armed timers (have %d)", n, c.pending(onlyNew))
		}
	}
}

// advance moves time forward and fires every timer that comes due, earliest first.
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	target := c.now.Add(d)
	c.mu.Unlock()

	for {
		c.mu.Lock()

		var next *fakeTimer

		for _, t := range c.timers {
			if t.armed && !t.at.After(target) && (next == nil || t.at.Before(next.at)) {
				next = t
			}
		}

		if next == nil {
			c.now = target
			c.mu.Unlock()

			return
		}

		next.armed = false
		c.now = next.at
		fire := c.now
		c.mu.Unlock()

		if next.fn != nil {
			next.fn()
		} else {
			next.ch <- fire
		}
	}
}
