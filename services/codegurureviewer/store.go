package codegurureviewer

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName     = "codeguru-reviewer"
	defaultDwell    = 500 * time.Millisecond
	defaultPageSize = 100
	maxPageSize     = 100
	maxTags         = 50
	maxTagKeyLen    = 128
	maxTagValueLen  = 256
)

// InMemoryBackend holds associations, reviews and feedback behind one coarse lock.
type InMemoryBackend struct {
	associations  *store.Table[associationRecord]
	reviews       *store.Table[reviewRecord]
	feedback      *store.Table[feedbackRecord]
	registry      *store.Registry
	mu            *lockmetrics.RWMutex
	now           func() time.Time
	appConfig     any
	accountID     string
	region        string
	dwellOverride time.Duration
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

	b.associations = store.Register(b.registry, "associations", store.New(associationKey))
	b.reviews = store.Register(b.registry, "reviews", store.New(reviewKey))
	b.feedback = store.Register(b.registry, "feedback", store.New(feedbackKey))

	return b
}

// SetClock replaces the time source.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// SetLifecycleDelay overrides how long associations and reviews stay in a transitional state.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.dwellOverride = d
}

// SetAppConfig records the service.AppContext.Config for lazy CodeCommit and S3 lookups.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.mu.Lock("SetAppConfig")
	defer b.mu.Unlock()

	b.appConfig = cfg
}

// Reset clears every table.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
}

func (b *InMemoryBackend) dwellLocked() time.Duration {
	if b.dwellOverride > 0 {
		return b.dwellOverride
	}

	return defaultDwell
}

func (b *InMemoryBackend) scope(ctx context.Context) (string, string) {
	account, region := b.accountID, b.region

	if m, ok := awsmeta.Key.Get(ctx); ok && m != nil {
		if m.Account != "" {
			account = m.Account
		}

		if m.Region != "" {
			region = m.Region
		}
	}

	return account, region
}

func (b *InMemoryBackend) associationARN(ctx context.Context, id string) string {
	account, region := b.scope(ctx)

	return arn.Build(serviceName, region, account, "association:"+id)
}

func callerUserID(ctx context.Context, fallback string) string {
	if m, ok := awsmeta.Key.Get(ctx); ok && m != nil && m.Principal != nil {
		if m.Principal.UserID != "" {
			return m.Principal.UserID
		}

		if m.Principal.Arn != "" {
			return m.Principal.Arn
		}
	}

	return fallback
}
