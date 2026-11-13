// Package ociretry implements OCI opc-retry-token replay bookkeeping.
package ociretry

import (
	"maps"
	"strings"
	"time"
)

// TTL is how long a token is remembered; the OCI SDK documents that retry tokens expire after 24 hours.
const TTL = 24 * time.Hour

// Entry is a remembered create result.
type Entry struct {
	At    time.Time `json:"at"`
	Value string    `json:"value"`
}

// Store maps scope|token to the first result. It has no lock: callers hold their backend lock.
type Store struct {
	m map[string]Entry
}

// New returns an empty store.
func New() *Store { return &Store{m: map[string]Entry{}} }

func key(scope, token string) string { return scope + "|" + token }

// Lookup returns the value remembered for the token if it has not expired.
func (s *Store) Lookup(scope, token string, now time.Time) (string, bool) {
	if token == "" {
		return "", false
	}

	e, ok := s.m[key(scope, token)]
	if !ok || now.Sub(e.At) >= TTL {
		return "", false
	}

	return e.Value, true
}

// Remember records the result of the first request that carried the token.
func (s *Store) Remember(scope, token, value string, now time.Time) {
	if token == "" {
		return
	}

	for k, e := range s.m {
		if now.Sub(e.At) >= TTL {
			delete(s.m, k)
		}
	}

	s.m[key(scope, token)] = Entry{At: now, Value: value}
}

// Entries returns the stored entries for a snapshot.
func (s *Store) Entries() map[string]Entry {
	out := make(map[string]Entry, len(s.m))
	maps.Copy(out, s.m)

	return out
}

// Load replaces the contents with a snapshot's entries.
func (s *Store) Load(m map[string]Entry) {
	s.m = make(map[string]Entry, len(m))
	maps.Copy(s.m, m)
}

// Reset empties the store.
func (s *Store) Reset() { s.m = map[string]Entry{} }

const sep = "|"

// Join packs a resource id and a work request id into one value.
func Join(id, workRequest string) string { return id + sep + workRequest }

// Split reverses Join.
func Split(v string) (string, string) {
	id, wr, _ := strings.Cut(v, sep)

	return id, wr
}
