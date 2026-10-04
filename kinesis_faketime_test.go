package main

import (
	"sync/atomic"
	"time"
)

// kinesisStreamSettleWait safely exceeds Kinesis's internal transient-state
// transition delay (streamTransitionDelay, 250ms in services/kinesis/models.go) so a
// single kinesisFakeClock.Advance call is guaranteed to move a CREATING/UPDATING
// stream past its ReadyAt deadline.
const kinesisStreamSettleWait = time.Second

// kinesisFakeClock is a goroutine-safe, manually-advanced clock for driving
// Kinesis's lazy stream-transition deadlines deterministically in tests that
// exercise it in-process (via InMemoryBackend.WithClock), including through
// another service's fire-and-forget delivery goroutine (e.g. DynamoDB's
// KinesisEmitter, EventBridge's target retry loop). Mirrors
// services/kinesis/faketime_test.go's fakeClock.
type kinesisFakeClock struct {
	now atomic.Pointer[time.Time]
}

// newKinesisFakeClock creates a kinesisFakeClock starting at start.
func newKinesisFakeClock(start time.Time) *kinesisFakeClock {
	c := &kinesisFakeClock{}
	c.now.Store(&start)

	return c
}

// Now returns the clock's current time. Suitable as InMemoryBackend.WithClock's argument.
func (c *kinesisFakeClock) Now() time.Time {
	return *c.now.Load()
}

// Advance moves the clock forward by d.
func (c *kinesisFakeClock) Advance(d time.Duration) {
	next := c.Now().Add(d)
	c.now.Store(&next)
}
