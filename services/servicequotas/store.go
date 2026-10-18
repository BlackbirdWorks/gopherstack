package servicequotas

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName     = "servicequotas"
	defaultDwell    = 500 * time.Millisecond
	defaultPageSize = 100
	maxPageSize     = 100
)

// OrgInfo describes the organization a management account belongs to.
type OrgInfo struct {
	ManagementAccountID string
	AllFeatures         bool
	ServiceAccess       bool
}

// OrgDirectory resolves the organization of an account.
type OrgDirectory interface {
	OrganizationOf(accountID string) (OrgInfo, bool)
}

// UsageSource reports current usage of a quota; ok is false when usage is not tracked.
type UsageSource interface {
	QuotaUsage(account, region, serviceCode, quotaCode string) (float64, bool)
}

// InMemoryBackend is the Service Quotas state; one coarse lock guards every table.
type InMemoryBackend struct {
	requests      *store.Table[Request]
	applied       *store.Table[AppliedValue]
	templates     *store.Table[TemplateItem]
	associations  *store.Table[Association]
	reports       *store.Table[Report]
	autoMgmt      *store.Table[AutoManagement]
	resourceTags  *store.Table[ResourceTags]
	registry      *store.Registry
	mu            *lockmetrics.RWMutex
	now           func() time.Time
	catalog       *Catalog
	orgs          OrgDirectory
	usage         UsageSource
	accountID     string
	region        string
	seq           int64
	dwellOverride time.Duration
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New(serviceName),
		now:       func() time.Time { return time.Now().UTC() },
		catalog:   defaultCatalog(),
		accountID: accountID,
		region:    region,
	}

	registerAllTables(b)

	return b
}

// Region returns the default region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the default account.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// SetLifecycleDelay overrides the dwell time of each quota request and report stage.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.dwellOverride = d
}

// SetOrgDirectory wires the Organizations lookup used by the quota request template.
func (b *InMemoryBackend) SetOrgDirectory(d OrgDirectory) {
	b.mu.Lock("SetOrgDirectory")
	defer b.mu.Unlock()

	b.orgs = d
}

// SetUsageSource wires the usage lookup used by utilization reports.
func (b *InMemoryBackend) SetUsageSource(u UsageSource) {
	b.mu.Lock("SetUsageSource")
	defer b.mu.Unlock()

	b.usage = u
}

// Reset clears every table.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.seq = 0
}

func (b *InMemoryBackend) dwell() time.Duration {
	if b.dwellOverride > 0 {
		return b.dwellOverride
	}

	return defaultDwell
}

func (b *InMemoryBackend) nextSeqLocked() int64 {
	b.seq++

	return b.seq
}

// scope is the account and Region a request runs in.
type scope struct{ account, region string }

func (b *InMemoryBackend) scopeOf(ctx context.Context) scope {
	s := scope{account: awsmeta.Account(ctx), region: awsmeta.Region(ctx)}
	if s.account == "" {
		s.account = b.accountID
	}

	if s.region == "" {
		s.region = b.region
	}

	return s
}

func (s scope) quotaARN(serviceCode, quotaCode string) string {
	return arn.Build(serviceName, s.region, s.account, serviceCode+"/"+quotaCode)
}

func defaultQuotaARN(region, serviceCode, quotaCode string) string {
	return arn.Build(serviceName, region, "", serviceCode+"/"+quotaCode)
}

func appliedKey(s scope, serviceCode, quotaCode string) string {
	return fmt.Sprintf("%s|%s|%s|%s", s.account, s.region, serviceCode, quotaCode)
}
