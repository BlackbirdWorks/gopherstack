package azurearm

import "strings"

// identityEntry is one resource that carries a managed identity block.
type identityEntry struct {
	id       string
	location string
	ident    resourceIdentity
}

// identityHolder is implemented by providers whose resources carry managed identities.
type identityHolder interface {
	identityEntries() []identityEntry
}

func (p *ComputeProvider) identityEntries() []identityEntry {
	p.mu.RLock("identityEntries")
	defer p.mu.RUnlock()

	out := make([]identityEntry, 0, len(p.items))

	for _, r := range p.items {
		if r.Identity.isSet() {
			out = append(out, identityEntry{id: r.armID(), location: r.Location, ident: r.Identity})
		}
	}

	return out
}

func (p *ContainerServiceProvider) identityEntries() []identityEntry {
	p.mu.RLock("identityEntries")
	defer p.mu.RUnlock()

	out := make([]identityEntry, 0, len(p.clusters))

	for _, c := range p.clusters {
		if c.Identity.isSet() {
			out = append(out, identityEntry{id: c.armID(), location: c.Location, ident: c.Identity})
		}
	}

	return out
}

func (p *ContainerAppsProvider) identityEntries() []identityEntry {
	p.mu.RLock("identityEntries")
	defer p.mu.RUnlock()

	var out []identityEntry

	for _, e := range p.envs {
		if e.Identity.isSet() {
			out = append(out, identityEntry{id: e.armID(), location: e.Location, ident: e.Identity})
		}
	}

	for _, a := range p.apps {
		if a.Identity.isSet() {
			id := acaResourceID(a.Sub, a.ResourceGroup, []string{acaAppType}, []string{a.Name})
			out = append(out, identityEntry{id: id.ARMID(), location: a.Location, ident: a.Identity})
		}
	}

	for _, j := range p.jobs {
		if j.Identity.isSet() {
			id := acaResourceID(j.Sub, j.ResourceGroup, []string{acaJobType}, []string{j.Name})
			out = append(out, identityEntry{id: id.ARMID(), location: j.Location, ident: j.Identity})
		}
	}

	return out
}

func (p *ContainerInstanceProvider) identityEntries() []identityEntry {
	p.mu.RLock("identityEntries")
	defer p.mu.RUnlock()

	var out []identityEntry

	for _, g := range p.groups {
		if g.Identity.isSet() {
			id := ResourceID{
				SubscriptionID: p.subOr(g.Sub), ResourceGroup: g.ResourceGroup,
				Namespace: namespaceMicrosoftContainerInstance, Types: []string{aciGroupsType}, Names: []string{g.Name},
			}
			out = append(out, identityEntry{id: id.ARMID(), location: g.Location, ident: g.Identity})
		}
	}

	return out
}

func (p *ContainerInstanceProvider) subOr(sub string) string {
	if sub != "" {
		return sub
	}

	return p.ids.defaultSub()
}

func (p *WebProvider) identityEntries() []identityEntry {
	p.mu.RLock("identityEntries")
	defer p.mu.RUnlock()

	var out []identityEntry

	add := func(s *storedWebSite, types, names []string) {
		if !s.Managed.isSet() {
			return
		}

		sub := s.Sub
		if sub == "" {
			sub = p.ids.defaultSub()
		}

		id := ResourceID{
			SubscriptionID: sub, ResourceGroup: s.ResourceGroup, Namespace: p.Namespace(), Types: types, Names: names,
		}
		out = append(out, identityEntry{id: id.ARMID(), location: s.Location, ident: s.Managed})
	}

	for _, s := range p.sites {
		add(s, []string{webSitesType}, []string{s.Name})

		for _, sl := range s.Slots {
			add(sl, []string{webSitesType, webSlotsType}, []string{s.Name, sl.Slot})
		}
	}

	return out
}

// identityHolders lists every registered provider that carries managed identities.
func (r *Registry) identityHolders() []identityHolder {
	var out []identityHolder

	for _, ns := range r.Providers() {
		if pr, ok := r.ProviderNamed(ns); ok {
			if h, isHolder := pr.(identityHolder); isHolder {
				out = append(out, h)
			}
		}
	}

	return out
}

// associatedResources returns the resources whose user-assigned identity list names userAssignedID.
func (r *Registry) associatedResources(userAssignedID string) []identityEntry {
	var out []identityEntry

	for _, h := range r.identityHolders() {
		for _, e := range h.identityEntries() {
			for _, ua := range e.ident.UserAssigned {
				if strings.EqualFold(ua, userAssignedID) {
					out = append(out, e)

					break
				}
			}
		}
	}

	return out
}

// systemIdentity returns the system-assigned identity of the resource with the given ARM id.
func (r *Registry) systemIdentity(armID string) (identityEntry, bool) {
	for _, h := range r.identityHolders() {
		for _, e := range h.identityEntries() {
			if strings.EqualFold(e.id, armID) && e.ident.hasSystem() {
				return e, true
			}
		}
	}

	return identityEntry{}, false
}
