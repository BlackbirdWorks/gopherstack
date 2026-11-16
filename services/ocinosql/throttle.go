package ocinosql

import (
	"net/http"
	"time"
)

type secondWindow struct {
	second int64
	reads  int
	writes int
}

func errThrottled(what string) *APIError {
	return newErr(http.StatusTooManyRequests, "TooManyRequests",
		"The provisioned "+what+" throughput of the table is exhausted; retry with backoff.")
}

func (w *secondWindow) roll(now time.Time) {
	if s := now.Unix(); s != w.second {
		*w = secondWindow{second: s}
	}
}

// throttle rejects an operation once the table's provisioned units for the current second are spent.
func (b *InMemoryBackend) throttle(t *Table, now time.Time, write bool) *APIError {
	if !b.enforceThroughput || t.Limits.Mode != modeProvisioned {
		return nil
	}

	t.window.roll(now)

	used, limit, what := t.window.reads, t.Limits.ReadUnits, "read"
	if write {
		used, limit, what = t.window.writes, t.Limits.WriteUnits, "write"
	}

	if used < limit {
		return nil
	}

	t.recordThrottle(now, write)

	return errThrottled(what)
}

func (t *Table) usageAt(now time.Time) *Usage {
	if t.Usage == nil {
		t.Usage = map[int64]*Usage{}
	}

	minute := now.Unix() / secondsPerMin * secondsPerMin

	u := t.Usage[minute]
	if u == nil {
		u = &Usage{}
		t.Usage[minute] = u
	}

	return u
}

func (t *Table) recordThrottle(now time.Time, write bool) {
	u := t.usageAt(now)
	if write {
		u.WriteThrottles++

		return
	}

	u.ReadThrottles++
}
