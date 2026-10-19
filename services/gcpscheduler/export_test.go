package gcpscheduler

import (
	"net/http"
	"time"
)

// SetClock replaces the backend clock.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// SetHTTPClient replaces the dispatch client.
func (b *InMemoryBackend) SetHTTPClient(c *http.Client) {
	b.mu.Lock("SetHTTPClient")
	defer b.mu.Unlock()

	b.client = c
}

// CronNext parses expr and returns its first fire time after t in the named zone.
func CronNext(expr, zone string, t time.Time) (time.Time, error) {
	s, err := parseCron(expr)
	if err != nil {
		return time.Time{}, err
	}

	loc, err := time.LoadLocation(zone)
	if err != nil {
		return time.Time{}, err
	}

	return s.next(t, loc), nil
}
