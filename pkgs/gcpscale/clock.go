// Package gcpscale holds the request-driven instance pool and injectable clock that Cloud Run style emulators share.
package gcpscale

import "time"

// Timer is the slice of *time.Timer that scaling uses.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// Clock abstracts time so idle scale-down and request queueing can be driven by tests.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
	AfterFunc(d time.Duration, f func()) Timer
}

// RealClock is the wall clock.
type RealClock struct{}

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time { return r.t.C }

func (r realTimer) Stop() bool { return r.t.Stop() }

// Now returns the wall time.
func (RealClock) Now() time.Time { return time.Now() }

// NewTimer starts a wall-clock timer.
func (RealClock) NewTimer(d time.Duration) Timer { return realTimer{t: time.NewTimer(d)} }

// AfterFunc runs f after d on the wall clock.
func (RealClock) AfterFunc(d time.Duration, f func()) Timer {
	return realTimer{t: time.AfterFunc(d, f)}
}
