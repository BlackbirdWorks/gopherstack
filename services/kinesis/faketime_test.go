package kinesis_test

import (
	"sync/atomic"
	"time"
)

// streamSettleWait safely exceeds Kinesis's internal transient-state
// transition delay (streamTransitionDelay, 250ms) so a single fakeClock.Advance
// call is guaranteed to move a CREATING/UPDATING/DELETING stream past its
// ReadyAt deadline.
const streamSettleWait = time.Second

// fakeClock is a goroutine-safe, manually-advanced clock for driving
// Kinesis's lazy stream-transition deadlines deterministically in tests,
// via InMemoryBackend.WithClock -- no real waiting, no time.Sleep. Tests
// that only ever touch the backend from a single goroutine (direct backend
// calls, or the httptest.NewRecorder in-process handler pattern) can use a
// plain captured variable instead (see stream_modes_test.go's fakeNow), but
// a test driving a real httptest.NewServer + AWS SDK client needs this: the
// server's own request-handling goroutine reads the clock concurrently with
// the test goroutine advancing it.
type fakeClock struct {
	now atomic.Pointer[time.Time]
}

// newFakeClock creates a fakeClock starting at start.
func newFakeClock(start time.Time) *fakeClock {
	c := &fakeClock{}
	c.now.Store(&start)

	return c
}

// Now returns the clock's current time. Suitable as InMemoryBackend.WithClock's argument.
func (c *fakeClock) Now() time.Time {
	return *c.now.Load()
}

// Advance moves the clock forward by d.
func (c *fakeClock) Advance(d time.Duration) {
	next := c.Now().Add(d)
	c.now.Store(&next)
}
