package azuregraph

import (
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// MicrosoftGraphAppID is the well-known application ID of the Microsoft Graph resource service principal.
const MicrosoftGraphAppID = "00000003-0000-0000-c000-000000000000"

// microsoftGraphSPID is the fixed object ID of the seeded Microsoft Graph service principal.
const microsoftGraphSPID = "7f0b5d3e-3c1f-4e6a-9d2b-5a8c1e4f6b20"

// Backend is the in-memory directory shared by the Graph listener and the ARM token endpoint.
type Backend struct {
	mu      *lockmetrics.RWMutex
	now     func() time.Time
	owners  map[string][]string
	members map[string][]string
	secrets map[string]string
	apps    store[Application]
	sps     store[ServicePrincipal]
	users   store[User]
	groups  store[Group]
	assigns store[AppRoleAssignment]
	grants  store[OAuth2PermissionGrant]
	domains []domain
	cfg     Settings
}

// Option customises a Backend.
type Option func(*Backend)

// WithClock replaces the wall clock.
func WithClock(now func() time.Time) Option { return func(b *Backend) { b.now = now } }

// NewBackend returns a directory seeded with the Microsoft Graph service principal.
func NewBackend(cfg Settings, opts ...Option) *Backend {
	if cfg.TenantID == "" {
		cfg.TenantID = DefaultTenantID
	}

	if cfg.DefaultDomain == "" {
		cfg.DefaultDomain = DefaultDomain
	}

	b := &Backend{mu: lockmetrics.New("azuregraph"), now: time.Now, cfg: cfg, domains: buildDomains(cfg)}
	for _, o := range opts {
		o(b)
	}

	b.reset()

	return b
}

// Reset drops all state and re-seeds the built-in service principals.
func (b *Backend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.reset()
}

func (b *Backend) reset() {
	b.apps, b.sps, b.users, b.groups = newStore[Application](), newStore[ServicePrincipal](), newStore[User](),
		newStore[Group]()
	b.assigns, b.grants = newStore[AppRoleAssignment](), newStore[OAuth2PermissionGrant]()
	b.owners, b.members, b.secrets = map[string][]string{}, map[string][]string{}, map[string]string{}
	b.seed()
}

// TenantID returns the directory's tenant.
func (b *Backend) TenantID() string { return b.cfg.TenantID }

func (b *Backend) stamp() time.Time { return b.now().UTC().Truncate(time.Second) }

func newID() string { return uuid.NewString() }

func lc(s string) string { return strings.ToLower(s) }

func isGUID(s string) bool {
	_, err := uuid.Parse(s)

	return err == nil && len(s) == len("00000000-0000-0000-0000-000000000000")
}

type dirObject interface {
	objID() string
	objKind() string
	objName() string
}

func (a *Application) objID() string        { return a.ID }
func (a *Application) objKind() string      { return kindApplication }
func (a *Application) objName() string      { return a.DisplayName }
func (s *ServicePrincipal) objID() string   { return s.ID }
func (s *ServicePrincipal) objKind() string { return kindServicePrincipal }
func (s *ServicePrincipal) objName() string { return s.DisplayName }
func (u *User) objID() string               { return u.ID }
func (u *User) objKind() string             { return kindUser }
func (u *User) objName() string             { return u.DisplayName }
func (g *Group) objID() string              { return g.ID }
func (g *Group) objKind() string            { return kindGroup }
func (g *Group) objName() string            { return g.DisplayName }

// lookup resolves any directory object by ID. Callers hold b.mu.
func (b *Backend) lookup(id string) (dirObject, bool) {
	id = lc(id)

	switch {
	case b.apps.items[id] != nil:
		return b.apps.items[id], true
	case b.sps.items[id] != nil:
		return b.sps.items[id], true
	case b.users.items[id] != nil:
		return b.users.items[id], true
	case b.groups.items[id] != nil:
		return b.groups.items[id], true
	}

	return nil, false
}

func (b *Backend) displayName(id string) string {
	if o, ok := b.lookup(id); ok {
		return o.objName()
	}

	return ""
}

type domain struct {
	Name      string
	IsDefault bool
	IsInitial bool
}

func buildDomains(cfg Settings) []domain {
	out := []domain{{Name: lc(cfg.DefaultDomain), IsDefault: true, IsInitial: strings.HasSuffix(lc(cfg.DefaultDomain), ".onmicrosoft.com")}}

	for _, d := range strings.Split(cfg.ExtraDomains, ",") {
		if d = lc(strings.TrimSpace(d)); d != "" && d != out[0].Name {
			out = append(out, domain{Name: d})
		}
	}

	return out
}

func (b *Backend) verifiedDomain(name string) bool {
	return slices.ContainsFunc(b.domains, func(d domain) bool { return d.Name == lc(name) })
}
