package azurearm

import (
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// SiteUserIdentity is a user-assigned identity attached to a site.
type SiteUserIdentity struct {
	ResourceID  string
	ClientID    string
	PrincipalID string
}

// SiteIdentity is the managed identity of a site or slot as the app's local token service needs it.
type SiteIdentity struct {
	TenantID          string
	SystemPrincipalID string
	User              []SiteUserIdentity
}

// SiteIdentity returns the managed identity of the data-plane app dpName (name, or name/slot).
func (r *Registry) SiteIdentity(dpName string) (SiteIdentity, bool) {
	p, ok := r.ProviderNamed(namespaceMicrosoftWeb)
	if !ok {
		return SiteIdentity{}, false
	}

	wp, ok := p.(*WebProvider)
	if !ok {
		return SiteIdentity{}, false
	}

	return wp.siteIdentityOf(dpName)
}

// identityCache is read by the app's token service while a swap holds the provider lock to warm a host, so it has
// its own lock and is refreshed from the site records on every change.
type identityCache struct {
	mu *lockmetrics.RWMutex
	m  map[string]SiteIdentity
	// rules are the production sites' traffic routing rules, cached for the same reason.
	rules map[string][]RampUpRule
}

func newIdentityCache() *identityCache {
	return &identityCache{
		mu:    lockmetrics.New("azurearm.webidentity"),
		m:     map[string]SiteIdentity{},
		rules: map[string][]RampUpRule{},
	}
}

func (p *WebProvider) siteIdentityOf(dpName string) (SiteIdentity, bool) {
	p.idCache.mu.RLock("siteIdentityOf")
	defer p.idCache.mu.RUnlock()

	id, ok := p.idCache.m[strings.ToLower(dpName)]

	return id, ok
}

// syncIdentities rebuilds the identity cache from the site records; callers hold p.mu.
func (p *WebProvider) syncIdentities() {
	next, rules := map[string]SiteIdentity{}, map[string][]RampUpRule{}

	for _, s := range p.sites {
		rules[strings.ToLower(s.Name)] = rampUpRulesOf(s)

		for _, rec := range append([]*storedWebSite{s}, slotList(s)...) {
			if rec.Managed.isSet() {
				next[strings.ToLower(rec.dpName())] = p.identityOf(rec)
			}
		}
	}

	p.idCache.mu.Lock("syncIdentities")
	p.idCache.m, p.idCache.rules = next, rules
	p.idCache.mu.Unlock()
}

func slotList(s *storedWebSite) []*storedWebSite {
	out := make([]*storedWebSite, 0, len(s.Slots))
	for _, sl := range s.Slots {
		out = append(out, sl)
	}

	return out
}

func (p *WebProvider) identityOf(rec *storedWebSite) SiteIdentity {
	out := SiteIdentity{TenantID: rec.Managed.TenantID}
	if out.TenantID == "" {
		out.TenantID = p.ids.tenantID()
	}

	if rec.Managed.hasSystem() {
		out.SystemPrincipalID = rec.Managed.PrincipalID
	}

	for _, id := range rec.Managed.UserAssigned {
		if principal, client, found := p.ids.lookup(id); found {
			out.User = append(out.User, SiteUserIdentity{ResourceID: id, ClientID: client, PrincipalID: principal})
		}
	}

	return out
}

func sameIdentity(a, b resourceIdentity) bool {
	return a.Type == b.Type && a.PrincipalID == b.PrincipalID && slices.Equal(a.UserAssigned, b.UserAssigned)
}

// restartOnIdentityChange restarts an app whose managed identity changed: App Service restarts the app and the
// host gets (or loses) IDENTITY_ENDPOINT and IDENTITY_HEADER (Learn: Managed identities, "Add a user-assigned
// identity").
func (p *WebProvider) restartOnIdentityChange(dpName string, prev *storedWebSite, now resourceIdentity) {
	if prev != nil && !sameIdentity(prev.Managed, now) {
		_ = p.dataPlane.Restart(dpName)
	}
}
