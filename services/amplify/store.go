package amplify

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// appIDChars is the character set used to generate Amplify app IDs.
const appIDChars = "abcdefghijklmnopqrstuvwxyz0123456789"

const (
	arnResourceApps     = "apps"
	arnResourceBranches = "branches"
)

// randomAppID generates an app ID shaped like the real ones: "d" plus 13 lowercase alphanumerics.
func randomAppID() string {
	const length = 14

	b := make([]byte, length)
	charCount := uint64(len(appIDChars))

	for i := range b {
		var v [8]byte
		_, _ = rand.Read(v[:])
		b[i] = appIDChars[binary.BigEndian.Uint64(v[:])%charCount]
	}

	b[0] = 'd'

	return string(b)
}

// randomID generates a cryptographically random 12-character alphanumeric ID.
func randomID() string {
	return randomAppID()
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
//
// Every resource collection is a *store.Table[T] registered on registry (see
// store_setup.go); branches, jobs, domains, and backendEnvironments were
// previously nested per-parent maps and are now flattened to a single Table
// keyed by a composite "parent|child" string with a companion byApp/byBranch
// [store.Index] for the "all children of parent X" lookups the nested maps
// used to answer directly. webhooksByApp was a reverse-lookup map
// (map[string][]string); it is now a [store.Index] on webhooks, not its own
// Table.
type InMemoryBackend struct {
	apps                     *store.Table[App]
	branches                 *store.Table[Branch]
	branchesByApp            *store.Index[Branch]
	jobs                     *store.Table[Job]
	jobsByBranch             *store.Index[Job]
	jobSeq                   map[string]int
	domains                  *store.Table[DomainAssociation]
	domainsByApp             *store.Index[DomainAssociation]
	webhooks                 *store.Table[Webhook]
	webhooksByApp            *store.Index[Webhook]
	backendEnvironments      *store.Table[BackendEnvironment]
	backendEnvironmentsByApp *store.Index[BackendEnvironment]
	artifacts                *store.Table[Artifact]
	artifactsByJob           *store.Index[Artifact]
	registry                 *store.Registry
	mu                       *lockmetrics.RWMutex
	accountID                string
	region                   string
}

// NewInMemoryBackend creates a new in-memory Amplify backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New("amplify"),
		accountID: accountID,
		region:    region,
		jobSeq:    make(map[string]int),
	}
	registerAllTables(b)

	return b
}

// compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// amplifyPaginate applies token-based pagination to a sorted slice of pointers.
func amplifyPaginate[T any](all []*T, nextToken string, maxResults int) ([]*T, string) {
	const defaultLimit = 100

	startIdx := 0
	if nextToken != "" {
		startIdx = page.DecodeToken(nextToken)
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
		outToken = page.EncodeToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken
}
