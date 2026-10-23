package connect

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName     = "connect"
	defaultPageSize = 100
	maxPageSize     = 1000
	maxTags         = 50
	maxTagKeyLen    = 128
	maxTagValueLen  = 256
	defaultDelay    = time.Second
)

// Resolvers lets Connect validate references to resources of sibling services.
type Resolvers interface {
	LambdaFunctionExists(ctx context.Context, functionARN string) bool
	BucketExists(ctx context.Context, bucket string) bool
	ObjectExists(ctx context.Context, bucket, key string) bool
}

// Integrations is the AppIntegrations surface behind EVENT and APPLICATION integration associations.
type Integrations interface {
	CreateEventAssociation(ctx context.Context, eventIntegrationName, clientID string) (string, error)
	DeleteEventAssociation(ctx context.Context, associationID string) error
	CreateApplicationAssociation(ctx context.Context, applicationID, clientID string) (string, error)
	DeleteApplicationAssociation(ctx context.Context, associationID string) error
}

// family is a table of instance-scoped records keyed by instance and resource ID.
type family[V any] struct {
	table  *store.Table[V]
	instOf func(*V) string
	idOf   func(*V) string
	arnOf  func(*V) string
	tagsOf func(*V) *map[string]string
	kind   string
}

func newFamily[V any](
	reg *store.Registry, name, kind string, instOf, idOf func(*V) string,
) family[V] {
	return family[V]{
		table:  store.Register(reg, name, store.New(func(v *V) string { return instOf(v) + "/" + idOf(v) })),
		kind:   kind,
		instOf: instOf,
		idOf:   idOf,
	}
}

func (f family[V]) get(inst, id string) (*V, error) {
	if v, ok := f.table.Get(inst + "/" + id); ok {
		return v, nil
	}

	return nil, notFoundErr(f.kind, id)
}

func (f family[V]) has(inst, id string) bool { return f.table.Has(inst + "/" + id) }

func (f family[V]) put(v *V) { f.table.Put(v) }

func (f family[V]) del(inst, id string) error {
	if !f.table.Delete(inst + "/" + id) {
		return notFoundErr(f.kind, id)
	}

	return nil
}

func (f family[V]) in(inst string) []*V {
	var out []*V

	f.table.Range(func(v *V) bool {
		if f.instOf(v) == inst {
			out = append(out, v)
		}

		return true
	})

	slices.SortFunc(out, func(a, b *V) int { return cmp.Compare(f.idOf(a), f.idOf(b)) })

	return out
}

func (f family[V]) dropInstance(inst string) {
	for _, v := range f.in(inst) {
		f.table.Delete(inst + "/" + f.idOf(v))
	}
}

func (f family[V]) findARN(resourceARN string) *map[string]string {
	if f.arnOf == nil {
		return nil
	}

	var out *map[string]string

	f.table.Range(func(v *V) bool {
		if f.arnOf(v) == resourceARN {
			out = f.tagsOf(v)

			return false
		}

		return true
	})

	return out
}

// Backend holds all Amazon Connect state behind one coarse lock.
type Backend struct {
	extras
	modules          family[moduleRec]
	hierarchyGroups  family[hierarchyGroupRec]
	integrations     family[integrationRec]
	contacts         family[contactRec]
	prompts          family[promptRec]
	agentStatuses    family[agentStatusRec]
	securityProfiles family[securityProfileRec]
	flows            family[flowRec]
	routingProfiles  family[routingProfileRec]
	users            family[userRec]
	hours            family[hoursRec]
	queues           family[queueRec]
	quickConnects    family[quickConnectRec]
	apps             Integrations
	resolvers        Resolvers
	hierarchyLevels  *store.Table[hierarchyStructureRec]
	mu               *lockmetrics.RWMutex
	instances        *store.Table[instanceRec]
	registry         *store.Registry
	now              func() time.Time
	phoneNumbers     *store.Table[phoneNumberRec]
	region           string
	accountID        string
	delay            time.Duration
}

// NewBackend creates an empty backend.
func NewBackend(accountID, region string) *Backend {
	b := &Backend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New(serviceName),
		now:       func() time.Time { return time.Now().UTC() },
		accountID: accountID,
		region:    region,
		delay:     defaultDelay,
	}
	b.registerTables()

	return b
}

// SetResolvers wires the sibling services used to validate references.
func (b *Backend) SetResolvers(r Resolvers) {
	b.mu.Lock("SetResolvers")
	defer b.mu.Unlock()

	b.resolvers = r
}

// SetIntegrations wires AppIntegrations for EVENT and APPLICATION integration associations.
func (b *Backend) SetIntegrations(i Integrations) {
	b.mu.Lock("SetIntegrations")
	defer b.mu.Unlock()

	b.apps = i
}

// SetLifecycleDelay overrides how long a new instance reports CREATION_IN_PROGRESS; zero keeps the built-in window.
func (b *Backend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	if d > 0 {
		b.delay = d
	}
}

// SetClock replaces the time source.
func (b *Backend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// Reset clears all state.
func (b *Backend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
}

func (b *Backend) scope(ctx context.Context) (string, string) {
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

func (b *Backend) buildARN(ctx context.Context, resource string) string {
	account, region := b.scope(ctx)

	return arn.Build(serviceName, region, account, resource)
}

func (b *Backend) stamp(ctx context.Context) Stamp {
	_, region := b.scope(ctx)
	t := awstime.NewEpochTime(b.now())

	return Stamp{LastModifiedTime: &t, LastModifiedRegion: region}
}

func checkName(field, v string, maxLen int) error {
	if strings.TrimSpace(v) == "" || len(v) > maxLen {
		return invalidParamErr("%s must be 1-%d characters", field, maxLen)
	}

	for _, r := range v {
		if unicode.IsControl(r) {
			return invalidParamErr("%s must not contain control characters", field)
		}
	}

	return nil
}

func checkDescription(v string) error {
	const maxLen = 250
	if len(v) > maxLen {
		return invalidParamErr("Description must be at most %d characters", maxLen)
	}

	return nil
}

func checkTags(tags map[string]string) error {
	if len(tags) > maxTags {
		return limitErr("a resource can have at most %d tags", maxTags)
	}

	for k, v := range tags {
		if k == "" || len(k) > maxTagKeyLen || len(v) > maxTagValueLen ||
			strings.HasPrefix(strings.ToLower(k), "aws:") {
			return invalidParamErr("tag %q is not valid", k)
		}
	}

	return nil
}

func paged[T any](all []T, token string, maxResults int32) ([]T, string, error) {
	p, err := restroute.Paginate(all, token, int(maxResults), defaultPageSize, maxPageSize)
	if err != nil {
		return nil, "", invalidParamErr("%v", err)
	}

	return p.Data, p.Next, nil
}

func cloneTags(t map[string]string) map[string]string { return maps.Clone(t) }

func orEmpty[T any](s []T) []T {
	if s == nil {
		return []T{}
	}

	return s
}

func oneOf(field, v string, allowed ...string) error {
	if !slices.Contains(allowed, v) {
		return invalidParamErr("%s must be one of %s", field, strings.Join(allowed, ", "))
	}

	return nil
}

func newID() string { return uuid.NewString() }

func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

// applyRename changes the name and description of a record, keeping names unique per instance.
func applyRename[V any](f family[V], v *V, name, desc *string, nameField, descField func(*V) *string) error {
	if name != nil {
		if err := checkName("Name", *name, maxNameLen); err != nil {
			return err
		}

		if nameExists(f, f.instOf(v), func(x *V) string { return *nameField(x) }, *name, f.idOf(v)) {
			return duplicateErr(f.kind, *name)
		}

		*nameField(v) = *name
	}

	if desc != nil {
		if err := checkDescription(*desc); err != nil {
			return err
		}

		*descField(v) = *desc
	}

	return nil
}
