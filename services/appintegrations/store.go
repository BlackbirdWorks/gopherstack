package appintegrations

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName     = "app-integrations"
	defaultPageSize = 50
	maxPageSize     = 50
	maxTags         = 200
	maxTagKeyLen    = 128
	maxTagValueLen  = 256
	maxNameLen      = 255
	maxDescLen      = 1000
)

var (
	namePattern   = regexp.MustCompile(`^[a-zA-Z0-9/._-]+$`)
	sourcePattern = regexp.MustCompile(`^aws\.partner/.*$`)
)

// EventRules is the EventBridge surface event integration associations drive.
type EventRules interface {
	BusExists(ctx context.Context, bus string) bool
	PutRule(ctx context.Context, bus, name, source string) error
	DeleteRule(ctx context.Context, bus, name string) error
}

// InMemoryBackend holds all AppIntegrations state behind one coarse lock.
type InMemoryBackend struct {
	registry    *store.Registry
	events      *store.Table[EventIntegration]
	eventAssocs *store.Table[EventIntegrationAssociation]
	dataIntegs  *store.Table[DataIntegration]
	dataAssocs  *store.Table[DataIntegrationAssociation]
	apps        *store.Table[Application]
	appAssocs   *store.Table[ApplicationAssociation]
	mu          *lockmetrics.RWMutex
	now         func() time.Time
	rules       EventRules
	accountID   string
	region      string
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

	b.events = store.Register(
		b.registry,
		"eventIntegrations",
		store.New(func(v *EventIntegration) string { return v.Name }),
	)
	b.eventAssocs = store.Register(b.registry, "eventAssociations",
		store.New(func(v *EventIntegrationAssociation) string { return v.EventIntegrationAssociationID }))
	b.dataIntegs = store.Register(
		b.registry,
		"dataIntegrations",
		store.New(func(v *DataIntegration) string { return v.ID }),
	)
	b.dataAssocs = store.Register(b.registry, "dataAssociations",
		store.New(func(v *DataIntegrationAssociation) string { return v.DataIntegrationAssociationID }))
	b.apps = store.Register(b.registry, "applications", store.New(func(v *Application) string { return v.ID }))
	b.appAssocs = store.Register(b.registry, "applicationAssociations",
		store.New(func(v *ApplicationAssociation) string { return v.AssociationID }))

	return b
}

// SetEventRules wires the EventBridge used to validate buses and own association rules.
func (b *InMemoryBackend) SetEventRules(r EventRules) {
	b.mu.Lock("SetEventRules")
	defer b.mu.Unlock()

	b.rules = r
}

// SetClock replaces the time source.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
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

func (b *InMemoryBackend) buildARN(ctx context.Context, resource string) string {
	account, region := b.scope(ctx)

	return arn.Build(serviceName, region, account, resource)
}

func checkName(field, v string) error {
	if len(v) == 0 || len(v) > maxNameLen || !namePattern.MatchString(v) {
		return invalidErr("%s must be 1-%d characters of letters, digits, '/', '.', '_' or '-'", field, maxNameLen)
	}

	return nil
}

func checkDescription(v string) error {
	if len(v) > maxDescLen {
		return invalidErr("Description must be at most %d characters", maxDescLen)
	}

	return nil
}

func checkTags(tags map[string]string) error {
	if len(tags) > maxTags {
		return quotaErr("a resource can have at most %d tags", maxTags)
	}

	for k, v := range tags {
		if k == "" || len(k) > maxTagKeyLen || len(v) > maxTagValueLen ||
			strings.HasPrefix(strings.ToLower(k), "aws:") {
			return invalidErr("tag %q is not valid", k)
		}
	}

	return nil
}

func page[T any](all []T, token string, maxResults int32) ([]T, string, error) {
	p, err := restroute.Paginate(all, token, int(maxResults), defaultPageSize, maxPageSize)
	if err != nil {
		return nil, "", invalidErr("%v", err)
	}

	return p.Data, p.Next, nil
}
