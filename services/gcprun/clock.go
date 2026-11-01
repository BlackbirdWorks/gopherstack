package gcprun

import "github.com/blackbirdworks/gopherstack/pkgs/gcpscale"

// Timer is the slice of *time.Timer that scaling and probing use.
type Timer = gcpscale.Timer

// Clock abstracts time so idle scale-down, request queueing and probe schedules can be driven by tests.
type Clock = gcpscale.Clock

type realClock = gcpscale.RealClock

type clockBox struct{ Clock }

// SetClock replaces the time source; nil restores the wall clock.
func (b *InMemoryBackend) SetClock(c Clock) {
	if c == nil {
		c = realClock{}
	}

	b.clock.Store(&clockBox{Clock: c})
}

func (b *InMemoryBackend) clk() Clock { return b.clock.Load().Clock }
