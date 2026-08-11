package neptune

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// nowISO8601 returns the current UTC time formatted as the ISO8601/RFC3339
// wire string Neptune's query/xml deserializers expect for *CreateTime
// fields (smithytime.ParseDateTime on the client side).
func nowISO8601() string {
	return time.Now().UTC().Format(time.RFC3339)
}

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

const (
	pgFamilyDefaultNeptune13 = "default.neptune1.3"
	snapshotSourceManual     = "manual"
	engineModeProvisioned    = "provisioned"
	engineModeServerless     = "serverless"
)

// neptunIdentifierRE validates Neptune resource identifiers:
// 1–63 chars, start with a letter, end with letter or digit, only letters/digits/hyphens,
// no consecutive hyphens.
var neptunIdentifierRE = regexp.MustCompile(`^[a-zA-Z](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$`)

// validateNeptuneIdentifier returns an error when id does not conform to Neptune naming rules.
func validateNeptuneIdentifier(id, fieldName string) error {
	const maxIdentifierLen = 63
	const invalidIdentifierMsg = "%w: %s %q is not a valid identifier; must start with a letter, " +
		"contain only letters/digits/hyphens, and not end with a hyphen"
	if id == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidParameter, fieldName)
	}
	if len(id) > maxIdentifierLen {
		return fmt.Errorf(
			"%w: %s %q exceeds maximum length of %d characters",
			ErrInvalidParameter, fieldName, id, maxIdentifierLen,
		)
	}
	if !neptunIdentifierRE.MatchString(id) {
		return fmt.Errorf(invalidIdentifierMsg, ErrInvalidParameter, fieldName, id)
	}
	if strings.Contains(id, "--") {
		return fmt.Errorf(
			"%w: %s %q cannot contain consecutive hyphens",
			ErrInvalidParameter, fieldName, id,
		)
	}

	return nil
}

const (
	defaultNeptunePort           = 8182
	defaultInstanceClass         = "db.r5.large"
	neptuneEngine                = "neptune"
	defaultEngineVersion         = "1.3.0.0"
	defaultBackupRetentionPeriod = 1
	clusterStatusAvailable       = "available"
	clusterStatusStopped         = "stopped"
	subscriptionStatusActive     = "active"
	maxPromotionTier             = 15
	maxTagsPerResource           = 50
	maxTagKeyLen                 = 128
	maxTagValueLen               = 256
	arnPartCount                 = 7
	endpointTypeReader           = "READER"
	endpointTypeWriter           = "WRITER"
	endpointTypeCustom           = "CUSTOM"
	endpointTypeAny              = "ANY"
	defaultMaintenanceWindow     = "sun:05:00-sun:06:00"
	defaultStorageType           = "aurora"
	defaultAllocatedStorage      = 1
	minBackupRetentionPeriod     = 1
	maxBackupRetentionPeriod     = 35
	minNeptunePort               = 1150
	maxNeptunePort               = 65535
	snapshotStatusAvailable      = "available"
	snapshotStatusCreating       = "creating"
	percentProgressComplete      = 100
	minFailoverClusterMembers    = 2
	engineVersion1200            = "1.2.0.0"
	networkTypeIPv4              = "IPV4"
)

// InMemoryBackend is a thread-safe in-memory backend for Neptune.
//
// Eight resource collections that were previously nested by region (outer key
// = region, e.g. map[string]map[string]*DBCluster) are now each a single flat
// *store.Table keyed by the composite "region|id" string (see store_setup.go),
// with a companion *store.Index grouping entries by region for per-region
// scans -- the same region-qualified-table pattern services/secretsmanager
// and services/cloudwatchlogs use. GlobalClusters are global/partition-scoped
// (like AWS) and were already flat, so they became a plain (non-composite-key)
// *store.Table. clusterRoles and tags remain raw nested maps: their values
// (a bare []string / []Tag) carry no identity of their own to key a
// store.Table by (see store_setup.go's doc comment for the full rationale).
type InMemoryBackend struct {
	registry                       *store.Registry
	clusters                       *store.Table[DBCluster]
	clustersByRegion               *store.Index[DBCluster]
	instances                      *store.Table[DBInstance]
	instancesByRegion              *store.Index[DBInstance]
	subnetGroups                   *store.Table[DBSubnetGroup]
	subnetGroupsByRegion           *store.Index[DBSubnetGroup]
	clusterParameterGroups         *store.Table[DBClusterParameterGroup]
	clusterParameterGroupsByRegion *store.Index[DBClusterParameterGroup]
	clusterSnapshots               *store.Table[DBClusterSnapshot]
	clusterSnapshotsByRegion       *store.Index[DBClusterSnapshot]
	parameterGroups                *store.Table[DBParameterGroup]
	parameterGroupsByRegion        *store.Index[DBParameterGroup]
	clusterEndpoints               *store.Table[DBClusterEndpoint]
	clusterEndpointsByRegion       *store.Index[DBClusterEndpoint]
	eventSubscriptions             *store.Table[EventSubscription]
	eventSubscriptionsByRegion     *store.Index[EventSubscription]
	globalClusters                 *store.Table[GlobalCluster] // global/partition-scoped, not region-nested
	clusterRoles                   map[string]map[string][]string
	tags                           map[string]map[string][]Tag
	// parameterOverrides and clusterParameterOverrides hold per-group
	// parameter value overrides written by Modify/Reset(DBCluster)ParameterGroup,
	// keyed by regionKey(region, groupName) -> parameter name -> value (see
	// parameter_catalog.go). Plain nested maps, not store.Table, following the
	// same rationale as clusterRoles/tags above: a bare ParameterValue carries
	// no identity of its own to key a table by.
	parameterOverrides map[string]map[string]ParameterValue
	// clusterParameterOverrides is the DBClusterParameterGroup counterpart of
	// parameterOverrides.
	clusterParameterOverrides map[string]map[string]ParameterValue
	// pendingMaintenanceActions holds queued maintenance actions keyed by
	// resource ARN -> action name -> action, seeded via
	// AddPendingMaintenanceActionInternal (see maintenance.go) since nothing
	// in this backend organically generates them the way real AWS does from
	// system-side upgrade/security-patch availability data.
	pendingMaintenanceActions map[string]map[string]PendingMaintenanceAction
	// eventsLog holds the account activity event log, keyed by region,
	// appended to by recordEvent at the point of the underlying state change
	// (see events.go). Bounded by maxEventsLogPerRegion to avoid unbounded
	// growth in a long-lived backend.
	eventsLog map[string][]Event
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new in-memory Neptune backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:                  store.NewRegistry(),
		clusterRoles:              make(map[string]map[string][]string),
		tags:                      make(map[string]map[string][]Tag),
		parameterOverrides:        make(map[string]map[string]ParameterValue),
		clusterParameterOverrides: make(map[string]map[string]ParameterValue),
		pendingMaintenanceActions: make(map[string]map[string]PendingMaintenanceAction),
		eventsLog:                 make(map[string][]Event),
		accountID:                 accountID,
		region:                    region,
		mu:                        lockmetrics.New("neptune"),
	}
	registerAllTables(b)

	return b
}

// Region returns the backend's AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// regionKey builds the composite store.Table primary key ("region|id") shared
// by every region-qualified table registered in store_setup.go.
func regionKey(region, id string) string { return region + "|" + id }

// resolveCopyDescription returns the target description for a copy operation,
// defaulting to the source's description when the requested target is empty.
func resolveCopyDescription(targetDescription, sourceDescription string) string {
	if targetDescription == "" {
		return sourceDescription
	}

	return targetDescription
}

// copyPreconditions validates the source/target names for a copy operation and
// returns the source value via get. notFound is returned when the source is
// missing; alreadyExists when the target already exists. get is a lookup
// closure (rather than a raw map) because store.Table does not expose its
// underlying map -- see e.g. CopyDBClusterParameterGroup's call site, which
// closes over the region to look up region-qualified keys.
func copyPreconditions[V any](
	get func(name string) (*V, bool),
	sourceName, targetName string,
	missingSourceMsg, missingTargetMsg string,
	notFound, alreadyExists error,
) (*V, error) {
	if sourceName == "" {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, missingSourceMsg)
	}

	if targetName == "" {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, missingTargetMsg)
	}

	src, exists := get(sourceName)
	if !exists {
		return nil, fmt.Errorf("%w: %s", notFound, sourceName)
	}

	if _, targetExists := get(targetName); targetExists {
		return nil, fmt.Errorf("%w: %s", alreadyExists, targetName)
	}

	return src, nil
}

// validNeptuneParameterGroupFamily returns true for known Neptune parameter group families.
func validNeptuneParameterGroupFamily(family string) bool {
	return family == pgFamilyNeptune12 || family == pgFamilyNeptune13 || family == "neptune1.4"
}

// AccountID returns the backend's AWS account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all backend state, returning it to a clean empty state.
//
// It calls b.registry.ResetAll() rather than re-registering tables:
// registerAllTables must run exactly once, at construction (store.Register
// panics on a duplicate name) -- see the doc comment on registerAllTables in
// store_setup.go.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()
	b.registry.ResetAll()
	b.clusterRoles = make(map[string]map[string][]string)
	b.tags = make(map[string]map[string][]Tag)
	b.parameterOverrides = make(map[string]map[string]ParameterValue)
	b.clusterParameterOverrides = make(map[string]map[string]ParameterValue)
	b.pendingMaintenanceActions = make(map[string]map[string]PendingMaintenanceAction)
	b.eventsLog = make(map[string][]Event)
}
