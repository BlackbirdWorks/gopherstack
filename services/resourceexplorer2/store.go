package resourceexplorer2

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName      = "resourceexplorer2"
	signingName      = "resource-explorer-2"
	servicePrincipal = "resource-explorer-2.amazonaws.com"
	defaultRegion    = "us-east-1"
	aggregatorLock   = 24 * time.Hour
)

// SourceResource is one resource the emulator can search.
type SourceResource struct {
	Tags map[string]string
	ARN  string
}

// ResourceSource enumerates the resources visible in the Region carried by ctx.
type ResourceSource interface {
	Resources(ctx context.Context) []SourceResource
}

// OrgAccess reports whether a service principal has organization-wide access enabled.
type OrgAccess interface {
	ServiceAccessEnabled(principal string) bool
}

// InMemoryBackend is the Resource Explorer state; one coarse lock guards every table.
type InMemoryBackend struct {
	indexes   *store.Table[Index]
	views     *store.Table[View]
	defaults  *store.Table[DefaultView]
	accounts  *store.Table[AccountState]
	tasks     *store.Table[SetupTask]
	registry  *store.Registry
	mu        *lockmetrics.RWMutex
	idem      *idempotency.Memo
	now       func() time.Time
	source    ResourceSource
	org       OrgAccess
	accountID string
	region    string
	dwell     time.Duration
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	if region == "" {
		region = defaultRegion
	}

	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New(serviceName),
		idem:      idempotency.New(serviceName),
		now:       func() time.Time { return time.Now().UTC() },
		accountID: accountID,
		region:    region,
	}

	b.indexes = store.Register(b.registry, "indexes", store.New(func(v *Index) string { return v.ARN }))
	b.views = store.Register(b.registry, "views", store.New(func(v *View) string { return v.ARN }))
	b.defaults = store.Register(b.registry, "defaultViews", store.New(func(v *DefaultView) string { return v.Key }))
	b.accounts = store.Register(b.registry, "accounts", store.New(func(v *AccountState) string { return v.AccountID }))
	b.tasks = store.Register(b.registry, "tasks", store.New(func(v *SetupTask) string { return v.ID }))

	return b
}

// Region returns the default Region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the default account.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// SetResourceSource wires the enumeration of searchable resources.
func (b *InMemoryBackend) SetResourceSource(s ResourceSource) {
	b.mu.Lock("SetResourceSource")
	defer b.mu.Unlock()

	b.source = s
}

// SetOrgAccess wires the Organizations service-access lookup.
func (b *InMemoryBackend) SetOrgAccess(o OrgAccess) {
	b.mu.Lock("SetOrgAccess")
	defer b.mu.Unlock()

	b.org = o
}

// SetLifecycleDelay keeps indexes in CREATING, UPDATING and DELETING for d; zero settles immediately.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.dwell = d
}

// Reset clears every table.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.idem.Clear()
}

func (b *InMemoryBackend) callerAccount(ctx context.Context) string {
	if a := awsmeta.Account(ctx); a != "" {
		return a
	}

	return b.accountID
}

func (b *InMemoryBackend) callerRegion(ctx context.Context) string {
	if r := awsmeta.Region(ctx); r != "" {
		return r
	}

	return b.region
}

func defaultKey(account, region string) string { return account + "|" + region }
