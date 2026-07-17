package serverlessrepo

import (
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend is an in-memory store for Serverless Application Repository resources.
//
// applications is a "clean" store.Table (registered directly on registry --
// see store_setup.go). appVersions, cfTemplates, and cfChangeSets are "dirty"
// tables: each was previously a map nested under appName, and each gained a
// hidden AppName field purely to key (and, for cfTemplates, index) itself --
// see the doc comments on those fields in this file. Because that field is
// json:"-", persistence.go round-trips them through an ephemeral DTO
// registry instead of registry.SnapshotAll()/RestoreAll() directly.
//
// appPolicies (map[string][]*ApplicationPolicyStatement) and appDependencies
// (map[string]map[string][]*ApplicationDependency) are left as plain maps:
// their values are slices, not *T, so they do not fit store.Table's
// keyed-by-identity-value shape. appPolicies is additionally order-sensitive
// (PutApplicationPolicy replaces the slice wholesale and GetApplicationPolicy
// returns it in that same order).
type InMemoryBackend struct {
	registry     *store.Registry
	applications *store.Table[Application]

	appVersions      *store.Table[ApplicationVersion]
	appVersionsByApp *store.Index[ApplicationVersion]

	cfTemplates      *store.Table[CloudFormationTemplate]
	cfTemplatesByApp *store.Index[CloudFormationTemplate]

	cfChangeSets      *store.Table[CloudFormationChangeSet]
	cfChangeSetsByApp *store.Index[CloudFormationChangeSet]

	// appPolicies maps appName -> []*ApplicationPolicyStatement
	appPolicies map[string][]*ApplicationPolicyStatement
	// appDependencies maps appName -> semanticVersion -> dependencies.
	appDependencies map[string]map[string][]*ApplicationDependency
	mu              *lockmetrics.RWMutex
	accountID       string
	region          string
}

// NewInMemoryBackend creates a new in-memory Serverless Application Repository backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:        store.NewRegistry(),
		appPolicies:     make(map[string][]*ApplicationPolicyStatement),
		appDependencies: make(map[string]map[string][]*ApplicationDependency),
		accountID:       accountID,
		region:          region,
		mu:              lockmetrics.New("serverlessrepo"),
	}
	registerAllTables(b)

	return b
}

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetTablesLocked()
	b.appPolicies = make(map[string][]*ApplicationPolicyStatement)
	b.appDependencies = make(map[string]map[string][]*ApplicationDependency)
}

// resetTablesLocked resets every store.Table-backed field: applications via
// the registry (the only "clean" table), and the three "dirty" tables
// individually since they are deliberately not registered on b.registry --
// see the InMemoryBackend doc comment. The caller MUST hold b.mu for writing.
func (b *InMemoryBackend) resetTablesLocked() {
	b.registry.ResetAll()
	b.appVersions.Reset()
	b.cfTemplates.Reset()
	b.cfChangeSets.Reset()
}

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }
