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
	resp        any
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
	if e, live, err := m.liveLocked(key, fingerprint); err != nil {
		return nil, err
	} else if live {
		if res, gerr := get(e.id); gerr == nil {
			return res, nil
		}
	}

	res, err := create()
	if err != nil {
		return nil, err
	}

	m.record(key, fingerprint, idOf(res), nil)

	return res, nil
}

func (m *Memo) liveLocked(key, fingerprint string) (entry, bool, error) {
	e, ok := m.entries[key]
	if !ok || !m.now().Before(e.expires) {
		return entry{}, false, nil
	}

	if e.fingerprint != fingerprint {
		return entry{}, false, ErrParamsMismatch
	}

	return e, true, nil
}

// Lookup returns the ID recorded for a live (op, token); ErrParamsMismatch when fingerprints differ.
func (m *Memo) Lookup(op, token, fingerprint string) (string, bool, error) {
	if token == "" {
		return "", false, nil
	}

	m.mu.RLock("Lookup")
	defer m.mu.RUnlock()

	e, live, err := m.liveLocked(op+"|"+token, fingerprint)

	return e.id, live, err
}

// Record remembers id as the resource (op, token) created.
func (m *Memo) Record(op, token, fingerprint, id string) {
	if token == "" {
		return
	}

	m.mu.Lock("Record")
	defer m.mu.Unlock()

	m.record(op+"|"+token, fingerprint, id, nil)
}

func (m *Memo) record(key, fingerprint, id string, resp any) {
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

	m.entries[key] = entry{expires: now.Add(m.ttl), fingerprint: fingerprint, id: id, resp: resp}
}

// Replay returns the response recorded for (op, token) or runs do and records its response.
// An empty token always runs do; a reused token with a different fingerprint yields ErrParamsMismatch.
func Replay[T any](m *Memo, op, token, fingerprint string, do func() (*T, error)) (*T, error) {
	if token == "" {
		return do()
	}

	m.mu.Lock("Replay")
	defer m.mu.Unlock()

	key := op + "|" + token
	if e, live, err := m.liveLocked(key, fingerprint); err != nil {
		return nil, err
	} else if live {
		if r, isT := e.resp.(*T); isT {
			return r, nil
		}
	}

	res, err := do()
	if err != nil {
		return nil, err
	}

	m.record(key, fingerprint, "", res)

	return res, nil
}

// Clear forgets every remembered token.
func (m *Memo) Clear() {
	m.mu.Lock("Clear")
	defer m.mu.Unlock()

	clear(m.entries)
}

// Len reports the number of remembered tokens.
func (m *Memo) Len() int {
	m.mu.RLock("Len")
	defer m.mu.RUnlock()

	return len(m.entries)
}
