// Package idempotency replays client-token creates: a retry with the same token and
// parameters returns the resource the first call made.
package idempotency

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	defaultTTL        = 5 * time.Minute
	defaultMaxEntries = 1024
)

// ErrParamsMismatch is returned when a token is reused with different parameters.
var ErrParamsMismatch = errors.New("client token was already used with different parameters")

type entry struct {
	expires     time.Time
	fingerprint string
	id          string
}

// Memo remembers which resource a (op, token) created; bounded by TTL and entry cap.
type Memo struct {
	now     func() time.Time
	mu      *lockmetrics.RWMutex
	entries map[string]entry
	ttl     time.Duration
	max     int
}

// Option configures a Memo.
type Option func(*Memo)

// WithClock overrides the time source.
func WithClock(now func() time.Time) Option { return func(m *Memo) { m.now = now } }

// WithLimits overrides the entry lifetime and cap.
func WithLimits(ttl time.Duration, maxEntries int) Option {
	return func(m *Memo) { m.ttl, m.max = ttl, maxEntries }
}

// New returns a Memo whose lock metrics are labelled name.
func New(name string, opts ...Option) *Memo {
	m := &Memo{
		now:     time.Now,
		mu:      lockmetrics.New(name + ".idempotency"),
		entries: make(map[string]entry),
		ttl:     defaultTTL,
		max:     defaultMaxEntries,
	}
	for _, o := range opts {
		o(m)
	}

	return m
}

// Fingerprint renders request parameters for comparison across retries.
func Fingerprint(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%+v", v)
	}

	return string(b)
}

// Create returns the live resource recorded for (op, token) or runs create and records it.
// An empty token always runs create; a reused token with a different fingerprint yields ErrParamsMismatch.
func Create[T any](
	m *Memo, op, token, fingerprint string,
	idOf func(*T) string, get func(string) (*T, error), create func() (*T, error),
) (*T, error) {
	if token == "" {
		return create()
	}

	m.mu.Lock("Create")
	defer m.mu.Unlock()

	key := op + "|" + token
	if e, ok := m.entries[key]; ok && m.now().Before(e.expires) {
		if e.fingerprint != fingerprint {
			return nil, ErrParamsMismatch
		}

		if res, err := get(e.id); err == nil {
			return res, nil
		}
	}

	res, err := create()
	if err != nil {
		return nil, err
	}

	m.record(key, fingerprint, idOf(res))

	return res, nil
}

func (m *Memo) record(key, fingerprint, id string) {
	now := m.now()

	for len(m.entries) >= m.max {
		var oldestKey string

		var oldest time.Time

		for k, e := range m.entries {
			if now.After(e.expires) {
				delete(m.entries, k)

				continue
			}

			if oldestKey == "" || e.expires.Before(oldest) {
				oldestKey, oldest = k, e.expires
			}
		}

		if len(m.entries) >= m.max {
			delete(m.entries, oldestKey)
		}
	}

	m.entries[key] = entry{expires: now.Add(m.ttl), fingerprint: fingerprint, id: id}
}

// Len reports the number of remembered tokens.
func (m *Memo) Len() int {
	m.mu.RLock("Len")
	defer m.mu.RUnlock()

	return len(m.entries)
}
