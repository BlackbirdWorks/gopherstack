package emrserverless

import (
	"crypto/rand"
	"encoding/binary"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	idChars  = "abcdefghijklmnopqrstuvwxyz0123456789"
	idLength = 10
)

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
	registry     *store.Registry
	mu           *lockmetrics.RWMutex
	accountID    string
	region       string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		sessionTokens:     make(map[string]map[string]string),
		applicationTokens: make(map[string]string),
		jobRunTokens:      make(map[string]map[string]string),
		accountID:         accountID,
		region:            region,
		registry:          store.NewRegistry(),
		mu:                lockmetrics.New("emrserverless"),
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
