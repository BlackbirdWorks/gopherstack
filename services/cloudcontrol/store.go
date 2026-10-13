// Package cloudcontrol provides an in-memory implementation of the AWS CloudControl API service.
package cloudcontrol

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// clientTokenTTL bounds how long a ClientToken is remembered for idempotent replay /
// conflict detection. Cloud Control API's docs don't state an explicit retention
// window, so this uses the repo's default for undocumented idempotency windows.
const clientTokenTTL = 24 * time.Hour

// clientTokenEntry records the fingerprint of the request that first used a
// given ClientToken, plus the RequestToken of the ProgressEvent it produced.
// Replaying the same ClientToken with a matching Fingerprint returns the
// cached event (idempotent replay, matching real CloudControl); replaying it
// with a different Fingerprint means the caller reused a ClientToken across
// a genuinely different request, which real CloudControl rejects with
// ClientTokenConflictException.
type clientTokenEntry struct {
	CreatedAt    time.Time `json:"createdAt,omitzero"`
	RequestToken string    `json:"requestToken"`
	Fingerprint  string    `json:"fingerprint"`
}

// InMemoryBackend is a thread-safe in-memory store for CloudControl resources.
type InMemoryBackend struct {
	registry     *store.Registry
	resources    *store.Table[Resource]      // key: typeName+"/"+identifier
	requests     *store.Table[ProgressEvent] // key: requestToken
	clientTokens map[string]clientTokenEntry // clientToken → entry (idempotency + conflict detection)
	typeHandlers map[string]TypeHandler
	mu           *lockmetrics.RWMutex
	now          func() time.Time
	accountID    string
	region       string
	opDelay      time.Duration
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		clientTokens: make(map[string]clientTokenEntry),
		typeHandlers: make(map[string]TypeHandler),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("cloudcontrol"),
		registry:     store.NewRegistry(),
		now:          time.Now,
	}
	registerAllTables(b)

	return b
}

// Region returns the region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state from the backend, returning it to a clean initial state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.clientTokens = make(map[string]clientTokenEntry)
}

// SetOperationDelay makes Create/Update/DeleteResource requests report IN_PROGRESS
// (cancellable) until d has elapsed. Zero, the default, completes them immediately.
func (b *InMemoryBackend) SetOperationDelay(d time.Duration) {
	b.mu.Lock("SetOperationDelay")
	defer b.mu.Unlock()

	b.opDelay = max(d, 0)
}

func (b *InMemoryBackend) clock() time.Time { return b.now() }

// settle marks event as in flight until the configured delay elapses. Callers hold b.mu.
func (b *InMemoryBackend) settle(event *ProgressEvent) {
	if b.opDelay > 0 {
		event.settleAt = event.EventTime.Add(b.opDelay)
	}
}

// present returns a copy of event as a caller should see it now: IN_PROGRESS with no
// resource model while the settle time has not passed, the stored terminal state after.
func (b *InMemoryBackend) present(event *ProgressEvent) *ProgressEvent {
	cp := copyEvent(event)
	if cp == nil || cp.settleAt.IsZero() || cp.OperationStatus != opStatusSuccess {
		return cp
	}

	if !b.clock().Before(cp.settleAt) {
		return cp
	}

	cp.OperationStatus = opStatusInProgress
	cp.ResourceModel = ""
	cp.RetryAfter = &unixEpochTime{cp.settleAt}

	return cp
}
