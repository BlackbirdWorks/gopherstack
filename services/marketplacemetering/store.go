package marketplacemetering

import (
	"context"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	ent "github.com/blackbirdworks/gopherstack/services/marketplaceentitlementservice"
)

const (
	serviceName = "marketplacemetering"

	defaultPublicKeyVersion = int32(1)
	maxProductCodeLen       = 255
	maxDimensionLen         = 255
	maxBatchRecords         = 25
	maxAllocations          = 2500
	maxAllocationTags       = 5
	maxTagKeyLen            = 100
	maxTagValueLen          = 256

	meterPastWindow     = 6 * time.Hour
	meterFutureWindow   = time.Hour
	tokenLifetime       = 4 * time.Hour
	keyRotationPeriod   = 90 * 24 * time.Hour
	recordIDLen         = 24
	customerIDLen       = 12
	accountIDLen        = 12
	statusSuccess       = "Success"
	statusNotSubscribed = "CustomerNotSubscribed"
	statusDuplicate     = "DuplicateRecord"
	opMeterUsage        = "MeterUsage"
	opBatchMeterUsage   = "BatchMeterUsage"
)

// EntitlementSink receives the entitlements derived from customer subscriptions.
type EntitlementSink interface {
	GrantEntitlement(e ent.Entitlement) error
	RevokeEntitlements(productCode, customerIdentifier string)
}

// InMemoryBackend holds products, subscriptions, usage and registrations behind one coarse lock.
type InMemoryBackend struct {
	products      *store.Table[Product]
	subscriptions *store.Table[Subscription]
	usage         *store.Table[StoredUsage]
	registrations *store.Table[Registration]
	registry      *store.Registry
	mu            *lockmetrics.RWMutex
	now           func() time.Time
	sink          EntitlementSink
	accountID     string
	region        string
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New(serviceName),
		now:       func() time.Time { return time.Now().UTC() },
		accountID: accountID,
		region:    region,
	}

	b.products = store.Register(b.registry, "products", store.New(productKey))
	b.subscriptions = store.Register(b.registry, "subscriptions", store.New(subscriptionKey))
	b.usage = store.Register(b.registry, "usage", store.New(usageKey))
	b.registrations = store.Register(b.registry, "registrations", store.New(registrationKey))

	return b
}

// Region returns the home region.
func (b *InMemoryBackend) Region() string { return b.region }

// SetClock replaces the time source.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// SetEntitlementSink wires the Marketplace Entitlement Service backend that receives subscription entitlements.
func (b *InMemoryBackend) SetEntitlementSink(s EntitlementSink) {
	b.mu.Lock("SetEntitlementSink")
	defer b.mu.Unlock()

	b.sink = s
}

// Reset clears every table.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
}

func (b *InMemoryBackend) callerAccount(ctx context.Context) string {
	if a := awsmeta.Account(ctx); a != "" {
		return a
	}

	return b.accountID
}

func usageDedupKey(product, customer, dimension string, unix int64) string {
	return product + "|" + customer + "|" + dimension + "|" + strconv.FormatInt(unix, 10)
}
