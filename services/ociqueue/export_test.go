package ociqueue

// MatchFilter compiles a consumer group filter and evaluates it against message attributes.
func MatchFilter(filter string, attrs map[string]string) (bool, error) {
	m, err := compileFilter(filter)
	if err != nil {
		return false, err
	}

	return m(attrs), nil
}

// LimitOverrides replaces service limits in tests; zero fields keep the documented value.
type LimitOverrides struct {
	Queues, Channels, InFlight, RequestsPerSec int
	QueueBytes, TenancyBytes, BytesPerSec      int64
}

func (b *InMemoryBackend) SetLimitsForTest(o LimitOverrides) {
	b.mu.Lock("SetLimitsForTest")
	defer b.mu.Unlock()

	set := func(dst *int, v int) {
		if v != 0 {
			*dst = v
		}
	}
	set64 := func(dst *int64, v int64) {
		if v != 0 {
			*dst = v
		}
	}

	set(&b.lim.queues, o.Queues)
	set(&b.lim.channels, o.Channels)
	set(&b.lim.inFlight, o.InFlight)
	set(&b.lim.requestsPerSec, o.RequestsPerSec)
	set64(&b.lim.queueBytes, o.QueueBytes)
	set64(&b.lim.tenancyBytes, o.TenancyBytes)
	set64(&b.lim.bytesPerSec, o.BytesPerSec)
}
