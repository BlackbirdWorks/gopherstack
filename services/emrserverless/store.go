package emrserverless

import (
	"crypto/rand"
	"encoding/binary"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	idChars  = "abcdefghijklmnopqrstuvwxyz0123456789"
	idLength = 10
)

// clientTokenTTL bounds how long a ClientToken is remembered for idempotent replay
// across applicationTokens/sessionTokens/jobRunTokens. AWS's docs don't state an
// explicit retention window, so this uses the repo's default for undocumented
// idempotency windows.
const clientTokenTTL = 24 * time.Hour

// InMemoryBackend stores EMR Serverless state in memory.
type InMemoryBackend struct {
	applications          *store.Table[Application]
	applicationsByARN     *store.Index[Application]
	jobRuns               *store.Table[JobRun]
	jobRunsByApplication  *store.Index[JobRun]
	jobRunsByARN          *store.Index[JobRun]
	sessions              *store.Table[Session]
	sessionsByApplication *store.Index[Session]
	sessionsByARN         *store.Index[Session]
	// sessionTokens maps applicationID -> clientToken -> sessionID. Left as a
	// plain map (not store.Table-backed): see store_setup.go's file doc.
	sessionTokens map[string]map[string]string
	// applicationTokens maps clientToken -> applicationID, giving
	// CreateApplication the same client-idempotency-token replay behavior
	// StartSession already has: a retried request (same clientToken) returns
	// the previously created application instead of erroring or duplicating.
	applicationTokens map[string]string
	// jobRunTokens maps applicationID -> clientToken -> jobRunID, giving
	// StartJobRun the same idempotency-token replay behavior as sessionTokens.
	jobRunTokens map[string]map[string]string
	// clientTokenCreatedAt tracks when each of the three token maps above wrote an
	// entry, flat-keyed by "domain\x00scope\x00token" (see clientTokenKey), so
	// sweepClientTokensLocked can bound their growth without changing the persisted
	// maps' value type. scope is the applicationID for session/jobrun tokens, "" for
	// application tokens (applicationTokens has no per-app nesting).
	clientTokenCreatedAt map[string]time.Time
	registry             *store.Registry
	mu                   *lockmetrics.RWMutex
	accountID            string
	region               string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		sessionTokens:        make(map[string]map[string]string),
		applicationTokens:    make(map[string]string),
		jobRunTokens:         make(map[string]map[string]string),
		clientTokenCreatedAt: make(map[string]time.Time),
		accountID:            accountID,
		region:               region,
		registry:             store.NewRegistry(),
		mu:                   lockmetrics.New("emrserverless"),
	}

	registerAllTables(b)

	return b
}

// Reset clears all backend state, returning it to the initial empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.sessionTokens = make(map[string]map[string]string)
	b.applicationTokens = make(map[string]string)
	b.jobRunTokens = make(map[string]map[string]string)
	b.clientTokenCreatedAt = make(map[string]time.Time)
}

// clientTokenKey builds the flat clientTokenCreatedAt key for a ClientToken
// belonging to one of the three token maps above.
func clientTokenKey(domain, scope, token string) string {
	return domain + "\x00" + scope + "\x00" + token
}

// touchClientToken sweeps expired entries out of tokens (a domain/scope's ClientToken
// dedup map) and records now as token's creation time, ahead of the caller inserting
// token into tokens. Caller must hold b.mu (write).
func (b *InMemoryBackend) touchClientToken(domain, scope, token string, tokens map[string]string, now time.Time) {
	for tok := range tokens {
		key := clientTokenKey(domain, scope, tok)

		ts, ok := b.clientTokenCreatedAt[key]
		if !ok || now.Sub(ts) >= clientTokenTTL {
			delete(tokens, tok)
			delete(b.clientTokenCreatedAt, key)
		}
	}

	b.clientTokenCreatedAt[clientTokenKey(domain, scope, token)] = now
}

// clientTokenFresh reports whether a previously-recorded token for domain/scope is
// still within clientTokenTTL (an entry with no recorded timestamp, e.g. restored from
// a pre-TTL snapshot, is treated as stale). Caller must hold b.mu (read or write).
func (b *InMemoryBackend) clientTokenFresh(domain, scope, token string, now time.Time) bool {
	ts, ok := b.clientTokenCreatedAt[clientTokenKey(domain, scope, token)]

	return ok && now.Sub(ts) < clientTokenTTL
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// newID generates a cryptographically random 10-character lowercase alphanumeric ID.
func newID() string {
	chars := []byte(idChars)
	charCount := uint64(len(chars))
	result := make([]byte, idLength)

	for i := range result {
		var v [8]byte
		_, _ = rand.Read(v[:])
		result[i] = chars[binary.BigEndian.Uint64(v[:])%charCount]
	}

	return string(result)
}

// emrPaginate applies token-based pagination to a sorted slice of pointers.
func emrPaginate[T any](all []*T, nextToken string, maxResults int) ([]*T, string) {
	const defaultLimit = 100

	startIdx := 0
	if nextToken != "" {
		if idx, err := strconv.Atoi(nextToken); err == nil && idx >= 0 {
			startIdx = idx
		}
	}

	if startIdx >= len(all) {
		return []*T{}, ""
	}

	limit := defaultLimit
	if maxResults > 0 {
		limit = maxResults
	}

	end := startIdx + limit

	var outToken string
	if end < len(all) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken
}
