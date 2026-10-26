package gcprun

import "time"

// Timer is the slice of *time.Timer that scaling and probing use.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// Clock abstracts time so idle scale-down, request queueing and probe schedules can be driven by tests.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
	AfterFunc(d time.Duration, f func()) Timer
}

type realClock struct{}

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time { return r.t.C }

func (r realTimer) Stop() bool { return r.t.Stop() }

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(d time.Duration) Timer { return realTimer{t: time.NewTimer(d)} }

func (realClock) AfterFunc(d time.Duration, f func()) Timer {
	return realTimer{t: time.AfterFunc(d, f)}
}

type clockBox struct{ Clock }

// SetClock replaces the time source; nil restores the wall clock.
func (b *InMemoryBackend) SetClock(c Clock) {
	if c == nil {
		c = realClock{}
	}

	b.clock.Store(&clockBox{Clock: c})
}

func (b *InMemoryBackend) clk() Clock { return b.clock.Load().Clock }
