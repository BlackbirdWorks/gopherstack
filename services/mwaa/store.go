package mwaa

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"strings"

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

const (
	defaultAirflowVersion      = "2.10.3"
	defaultEnvironmentClass    = "mw1.small"
	defaultMaxWorkers          = int32(10)
	defaultMinWorkers          = int32(1)
	defaultMaxWebservers       = int32(2)
	defaultMinWebservers       = int32(2)
	minWebserversAllowed       = int32(1)
	maxWebserversAllowed       = int32(5)
	defaultSchedulersV2        = int32(2)
	defaultSchedulersV1        = int32(1)
	minSchedulersV2            = int32(2)
	maxSchedulersV2            = int32(5)
	defaultWebserverAccessMode = "PUBLIC_ONLY"
	restAPISuccessCode         = int32(200)
	maxMetricsPerEnv           = 1000
	listEnvDefaultPageSize     = 25
	listEnvMaxPageSize         = 100

	// Environment status constants (AWS: CREATING | CREATE_FAILED | AVAILABLE |
	// UPDATING | DELETING | DELETED | UNAVAILABLE | UPDATE_FAILED |
	// ROLLING_BACK | CREATING_SNAPSHOT | PENDING | MAINTENANCE -- confirmed via
	// aws-sdk-go-v2/service/mwaa/types.EnvironmentStatus's enum values.
	// gopherstack previously used the fabricated "UPDATE_ROLLING_BACK" (the
	// real value is "ROLLING_BACK") and an invented "ERROR" status not in the
	// real enum at all.
	envStatusAvailable        = "AVAILABLE"
	envStatusCreating         = "CREATING"
	envStatusCreatingSnapshot = "CREATING_SNAPSHOT"
	envStatusDeleting         = "DELETING"
	envStatusDeleted          = "DELETED"
	envStatusUpdating         = "UPDATING"
	envStatusRollingBack      = "ROLLING_BACK"
	envStatusUpdateFailed     = "UPDATE_FAILED"
	envStatusPending          = "PENDING"
	envStatusCreateFailed     = "CREATE_FAILED"
	envStatusUnavailable      = "UNAVAILABLE"

	// EndpointManagement constants.
	endpointManagementService  = "SERVICE"
	endpointManagementCustomer = "CUSTOMER"

	// WebserverAccessMode constants (AWS: PUBLIC_ONLY | PRIVATE_ONLY | PUBLIC_AND_PRIVATE
	// -- confirmed via aws-sdk-go-v2/service/mwaa/types.WebserverAccessMode's enum values).
	accessModePublic           = "PUBLIC_ONLY"
	accessModePrivate          = "PRIVATE_ONLY"
	accessModePublicAndPrivate = "PUBLIC_AND_PRIVATE"

	// Worker limits.
	maxWorkersAllowed = int32(25)

	// NetworkConfiguration.SecurityGroupIds bounds (AWS: 1-5 entries).
	minSecurityGroupIDs = 1
	maxSecurityGroupIDs = 5

	// NetworkConfiguration.SubnetIds is a fixed-size list of exactly 2 entries
	// (subnets must span exactly 2 Availability Zones).
	requiredSubnetIDs = 2

	// Tag limit per resource.
	maxTagsPerResource = 50

	// Environment name length bounds.
	minEnvNameLen = 1
	maxEnvNameLen = 80

	// WorkerReplacementStrategy constants (AWS: FORCED | GRACEFUL -- confirmed
	// via aws-sdk-go-v2/service/mwaa/types.WorkerReplacementStrategy's enum
	// values; gopherstack previously fabricated "TERMINATION_WITH_DRAIN",
	// which is not a real value, while rejecting the real "GRACEFUL").
	workerStrategyForced   = "FORCED"
	workerStrategyGraceful = "GRACEFUL"
)

// generateMWAAToken produces a JWT-shaped token for CLI and web login operations.
// The token is deterministic for a given (envName, kind) pair so tests can reason
// about its structure; it is NOT a cryptographically valid JWT.
func generateMWAAToken(envName, kind string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString(
		[]byte(`{"env":"` + envName + `","type":"` + kind + `"}`),
	)

	sum := sha256.Sum256([]byte(envName + ":" + kind))
	sig := base64.RawURLEncoding.EncodeToString(sum[:])

	return header + "." + payload + "." + sig
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	// environments is a flat store.Table keyed by the composite "region|name"
	// string (see regionKey below), replacing the old map[string]map[string]*Environment
	// nesting (outer key = region). environmentsByRegion and environmentsByARN
	// are companion secondary indexes -- see store_setup.go.
	environments         *store.Table[Environment]
	environmentsByRegion *store.Index[Environment]
	environmentsByARN    *store.Index[Environment]
	registry             *store.Registry
	// metrics remains a plain nested map (region → env name → metrics): its
	// values are bare []MetricDatum slices with no identity of their own, so
	// it is not a candidate for store.Table -- see store_setup.go.
	metrics   map[string]map[string][]MetricDatum
	mu        *lockmetrics.RWMutex
	region    string
	accountID string
}

// NewInMemoryBackend creates a new MWAA in-memory backend.
func NewInMemoryBackend(region, accountID string) *InMemoryBackend {
	b := &InMemoryBackend{
		region:    region,
		accountID: accountID,
		registry:  store.NewRegistry(),
		metrics:   make(map[string]map[string][]MetricDatum),
		mu:        lockmetrics.New("mwaa"),
	}

	registerAllTables(b)

	return b
}

// regionKey builds the composite store.Table primary key ("region|id") used
// by the environments table and its byARN index.
func regionKey(region, id string) string { return region + "|" + id }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// metricsStore returns the metrics map for the given region, lazily creating it.
// Callers must hold b.mu.
func (b *InMemoryBackend) metricsStore(region string) map[string][]MetricDatum {
	if b.metrics[region] == nil {
		b.metrics[region] = make(map[string][]MetricDatum)
	}

	return b.metrics[region]
}

// metricsStoreRO returns the region-scoped metrics map for region without
// mutating the outer map. Safe to call while holding only b.mu.RLock(): if
// the region has not been observed yet, it returns a fresh, unregistered,
// empty map instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) metricsStoreRO(region string) map[string][]MetricDatum {
	if v := b.metrics[region]; v != nil {
		return v
	}

	return map[string][]MetricDatum{}
}

// Reset closes the current mutex and reinitialises all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Close()
	b.mu = lockmetrics.New("mwaa")
	b.registry.ResetAll()
	b.metrics = make(map[string]map[string][]MetricDatum)
}

// AddEnvironmentInternal creates an environment with minimal defaults, bypassing
// validation, intended for use in tests only. It uses the backend's default region.
func (b *InMemoryBackend) AddEnvironmentInternal(name string) *Environment {
	return b.AddEnvironmentInternalRegion(b.region, name)
}

// AddEnvironmentInternalRegion creates an environment with minimal defaults in the
// given region, bypassing validation, intended for use in tests only.
func (b *InMemoryBackend) AddEnvironmentInternalRegion(region, name string) *Environment {
	b.mu.Lock("AddEnvironmentInternal")
	defer b.mu.Unlock()

	envARN := arn.Build("airflow", region, b.accountID, "environment/"+name)
	env := &Environment{
		region:    region,
		Name:      name,
		ARN:       envARN,
		Status:    envStatusAvailable,
		Tags:      make(map[string]string),
		CreatedAt: epochSecondsNow(),
	}

	b.environments.Put(env)

	return env
}

// findByARN looks up an environment in the given region by its ARN using the
// environments table's byARN index. Must be called with lock held.
func (b *InMemoryBackend) findByARN(region, resourceARN string) *Environment {
	matches := b.environmentsByARN.Get(regionKey(region, resourceARN))
	if len(matches) == 0 {
		return nil
	}

	return matches[0]
}

// webserverHostname extracts the bare hostname from an environment's WebserverURL.
// The URL is stored as "https://hostname" (no trailing slash, no path), so this
// strips the scheme prefix to match the AWS CLI/web-login token wire format.
func webserverHostname(webserverURL string) string {
	return strings.TrimPrefix(webserverURL, "https://")
}
