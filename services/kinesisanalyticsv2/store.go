package kinesisanalyticsv2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// regionFromARN extracts the region component (index 3) from an AWS ARN
// (arn:partition:service:region:account:resource), falling back to defaultRegion.
func regionFromARN(resourceARN, defaultRegion string) string {
	parts := strings.Split(resourceARN, ":")
	const regionIndex = 3
	if len(parts) > regionIndex && parts[regionIndex] != "" {
		return parts[regionIndex]
	}

	return defaultRegion
}

const kav2DefaultPageSize = 50

// InMemoryBackend stores Kinesis Data Analytics v2 state in memory.
//
// applications and snapshots are store.Table-backed (Phase 3.3); see
// store_setup.go for the composite keys and secondary indexes that replace
// the pre-Phase-3.3 nested map[region]map[name]* layout. operations and
// versions are left as plain nested maps of slices: both are order-sensitive
// append histories (versions is read by positional index in
// RollbackApplication; operations is returned in insertion order with no
// explicit sort), and store.Index does not preserve insertion order, so
// neither fits a store.Table+Index conversion -- see pkgs/store's package
// doc and .claude/memories/pkgs-catalog.md. Neither was persisted before
// this conversion and neither is persisted after it (see persistence.go).
type InMemoryBackend struct {
	applications         *store.Table[Application]
	applicationsByRegion *store.Index[Application]
	applicationsByARN    *store.Index[Application]
	snapshots            *store.Table[Snapshot]
	snapshotsByApp       *store.Index[Snapshot]
	registry             *store.Registry
	operations           map[string]map[string][]*ApplicationOperation // region → applicationName → []Operation
	versions             map[string]map[string][]*Application          // region → applicationName → []version
	mu                   *lockmetrics.RWMutex
	accountID            string
	defaultRegion        string
	nextID               int64
}

// NewInMemoryBackend creates a new in-memory Kinesis Data Analytics v2 backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:      store.NewRegistry(),
		operations:    make(map[string]map[string][]*ApplicationOperation),
		versions:      make(map[string]map[string][]*Application),
		mu:            lockmetrics.New("kinesisanalyticsv2"),
		accountID:     accountID,
		defaultRegion: region,
	}
	registerAllTables(b)

	return b
}

// Region returns the backend default region.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// AccountID returns the backend account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// --- Per-region store accessors (callers must hold b.mu) ---

// findApplication looks up an application by region and name.
func (b *InMemoryBackend) findApplication(region, name string) (*Application, bool) {
	return b.applications.Get(applicationKey(region, name))
}

// versionsStore returns the version map for region, lazily creating it.
func (b *InMemoryBackend) versionsStore(region string) map[string][]*Application {
	if b.versions[region] == nil {
		b.versions[region] = make(map[string][]*Application)
	}

	return b.versions[region]
}

// applicationARN builds an ARN for a Kinesis Data Analytics v2 application.
func (b *InMemoryBackend) applicationARN(region, name string) string {
	return arn.Build("kinesisanalytics", region, b.accountID, "application/"+name)
}

// GenerateApplicationARN exposes the ARN builder for testing.
func (b *InMemoryBackend) GenerateApplicationARN(name string) string {
	return b.applicationARN(b.defaultRegion, name)
}

// Reset clears all state and resets the ID counter.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.operations = make(map[string]map[string][]*ApplicationOperation)
	b.versions = make(map[string]map[string][]*Application)
	b.nextID = 0
}

// AddApplicationInternal is a test-only seed helper that stores an application directly.
func (b *InMemoryBackend) AddApplicationInternal(ctx context.Context, app *Application) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationInternal")
	defer b.mu.Unlock()

	cp := appCopy(app)
	cp.Region = region
	b.applications.Put(cp)
}

// newResourceID generates a unique resource ID. Must be called under b.mu.
func (b *InMemoryBackend) newResourceID(prefix string) string {
	b.nextID++

	return fmt.Sprintf("%s-%d", prefix, b.nextID)
}

// checkAndBumpVersion validates the current version and increments it.
// A zero/negative currentVersionID is treated as "skip version check".
// Callers must follow a successful call with `defer b.snapshotVersion(region,
// name, app)` (see below) to record the version history.
func checkAndBumpVersion(app *Application, currentVersionID int64) error {
	if currentVersionID > 0 && app.ApplicationVersionID != currentVersionID {
		return ErrConcurrentModification
	}

	bumpVersion(app)

	return nil
}

// bumpVersion increments app.ApplicationVersionID and updates every
// version-lineage field real AWS's ApplicationDetail exposes for a
// version-bumping change: LastUpdateTimestamp,
// ApplicationVersionCreateTimestamp, and ApplicationVersionUpdatedFrom (the
// version immediately before this bump). It also clears
// ApplicationVersionRolledBackFrom/To, since only RollbackApplication (which
// bumps the version itself rather than calling this helper) sets those, and
// any subsequent non-rollback change means the current version is no longer
// "the result of" a prior rollback. Must be called under b.mu.
func bumpVersion(app *Application) {
	prev := app.ApplicationVersionID
	now := time.Now().UTC()

	app.ApplicationVersionID++
	app.LastUpdateTimestamp = now
	app.ApplicationVersionCreateTimestamp = now
	app.ApplicationVersionUpdatedFrom = &prev
	app.ApplicationVersionRolledBackFrom = nil
	app.ApplicationVersionRolledBackTo = nil
}

// conditionalToken deterministically derives the opaque concurrency token
// real AWS returns as ApplicationDetail.ConditionalToken (an alternative to
// CurrentApplicationVersionId for UpdateApplication's optimistic-concurrency
// check -- see UpdateApplicationParams.ConditionalToken). Tying it 1:1 to
// (ARN, ApplicationVersionId) means it automatically changes on every
// version bump without needing a separate stored field to keep in sync.
func conditionalToken(app *Application) string {
	sum := sha256.Sum256([]byte(app.ApplicationARN + "#" + strconv.FormatInt(app.ApplicationVersionID, 10)))

	return hex.EncodeToString(sum[:16])
}

// checkAndBumpVersionOrToken validates the current version and/or
// ConditionalToken and increments the version. A provided conditionalToken
// takes precedence over currentVersionID when non-empty, matching real AWS's
// documented preference ("use the ConditionalToken parameter instead of
// CurrentApplicationVersionId"). A zero/negative currentVersionID and an
// empty conditionalToken both mean "skip that particular check" -- if
// neither is supplied, the update proceeds unconditionally (leniency,
// matching every other Add*/Delete* op's optional CurrentApplicationVersionId
// convention in this package). Callers must follow a successful call with
// `defer b.snapshotVersion(region, name, app)`, exactly like
// checkAndBumpVersion.
func checkAndBumpVersionOrToken(app *Application, currentVersionID int64, conditionalTok string) error {
	if conditionalTok != "" && conditionalTok != conditionalToken(app) {
		return ErrConcurrentModification
	}

	if conditionalTok == "" {
		return checkAndBumpVersion(app, currentVersionID)
	}

	bumpVersion(app)

	return nil
}

// snapshotVersion appends a copy of app's current state to the (region,
// name) version history. Callers invoke this via `defer` immediately after a
// successful checkAndBumpVersion so it captures state *after* the caller's
// subsequent field mutations run, not the state at the moment of the version
// bump -- deferred calls execute at function return, once all of the
// function's own statements (including those mutations) have completed.
// Without this, DescribeApplicationVersion, ListApplicationVersions, and
// RollbackApplication would only ever see the single version-1 snapshot
// CreateApplication seeds -- every Add*/Delete* config op and
// UpdateApplication bumps ApplicationVersionID but previously left
// b.versions untouched, so RollbackApplication's "len(vers) < 2" guard could
// never be satisfied by real traffic and always failed with ErrValidation.
// Must run under b.mu.
func (b *InMemoryBackend) snapshotVersion(region, name string, app *Application) {
	b.versionsStore(region)[name] = append(b.versionsStore(region)[name], appCopy(app))
}

// recordOperation appends a completed ApplicationOperation record for
// (region, appName) and returns its OperationID, so DescribeApplicationOperation
// and ListApplicationOperations have real data to serve instead of being
// permanently empty. Must be called under b.mu.
func (b *InMemoryBackend) recordOperation(region, appName, opType string) string {
	now := time.Now().UTC()
	opID := b.newResourceID("op")

	op := &ApplicationOperation{
		OperationID:     opID,
		ApplicationName: appName,
		Operation:       opType,
		OperationStatus: OperationStatusSuccessful,
		StartTimestamp:  now,
		EndTimestamp:    now,
	}

	if b.operations[region] == nil {
		b.operations[region] = make(map[string][]*ApplicationOperation)
	}

	b.operations[region][appName] = append(b.operations[region][appName], op)

	return opID
}

// parseNextToken parses a pagination token (integer offset) into a slice index.
func parseNextToken(token string) int {
	if token == "" {
		return 0
	}

	idx, err := strconv.Atoi(token)
	if err != nil || idx < 0 {
		return 0
	}

	return idx
}
