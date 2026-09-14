package polly

import (
	"fmt"
	"time"
)

// WithClock overrides the backend's time source, used by tests to drive
// StartSpeechSynthesisStream's sliding throttle window (checkStreamThrottleLocked)
// deterministically -- no time.Sleep, no real wall-clock waits.
func (b *InMemoryBackend) WithClock(now func() time.Time) *InMemoryBackend {
	if now != nil {
		b.nowFunc = now
	}

	return b
}

// WithStreamLimits overrides StartSpeechSynthesisStream's throttle/concurrency
// caps (see limits.go). A zero argument keeps its real-AWS default -- lets
// tests trip the cap without issuing defaultStreamTPS/defaultStreamConcurrency
// real requests.
func (b *InMemoryBackend) WithStreamLimits(tps, concurrency int) *InMemoryBackend {
	if tps > 0 {
		b.streamLimits.tps = tps
	}

	if concurrency > 0 {
		b.streamLimits.concurrency = concurrency
	}

	return b
}

// BeginSpeechSynthesisStream enforces StartSpeechSynthesisStream's real AWS
// quotas (limits.go) before a request proceeds. The concurrent-request cap
// is checked first: an unreleased slot only ever comes from a request that
// is still in flight, a stronger overload signal than one more tick in the
// rate window. On success the caller MUST invoke the returned release func
// exactly once when the request finishes (defer works); on error the
// returned func is nil.
func (b *InMemoryBackend) BeginSpeechSynthesisStream(engine string) (func(), error) {
	b.mu.Lock("StartSpeechSynthesisStream")
	defer b.mu.Unlock()

	if b.streamsInFlight >= b.streamLimits.concurrency {
		return nil, fmt.Errorf(
			"%w: StartSpeechSynthesisStream concurrent-request limit of %d exceeded",
			ErrServiceQuotaExceeded, b.streamLimits.concurrency,
		)
	}

	if err := b.checkStreamThrottleLocked(engine); err != nil {
		return nil, err
	}

	b.streamsInFlight++

	return b.endSpeechSynthesisStream, nil
}

func (b *InMemoryBackend) endSpeechSynthesisStream() {
	b.mu.Lock("StartSpeechSynthesisStream")
	defer b.mu.Unlock()

	if b.streamsInFlight > 0 {
		b.streamsInFlight--
	}
}

// checkStreamThrottleLocked enforces the per-engine sliding one-second
// transaction-rate window (limits.go's streamLimits.tps). Keyed by engine so
// a future op-wide reuse of this mechanism can't let one engine's traffic
// throttle another's, even though StartSpeechSynthesisStream itself only
// ever calls this with engine="generative" (the sole Engine value the
// handler accepts for this op).
//
// The caller MUST hold b.mu for writing.
func (b *InMemoryBackend) checkStreamThrottleLocked(engine string) error {
	now := b.nowFunc()
	cutoff := now.Add(-time.Second)

	kept := b.streamRequests[engine][:0]
	for _, t := range b.streamRequests[engine] {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}

	if len(kept) >= b.streamLimits.tps {
		b.streamRequests[engine] = kept

		return fmt.Errorf(
			"%w: StartSpeechSynthesisStream rate of %d transactions per second exceeded for engine %q",
			ErrThrottling, b.streamLimits.tps, engine,
		)
	}

	b.streamRequests[engine] = append(kept, now)

	return nil
}
