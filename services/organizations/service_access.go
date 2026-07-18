package organizations

import (
	"cmp"
	"slices"
	"time"
)

// EnableAWSServiceAccess enables a service principal for org-wide access.
func (b *InMemoryBackend) EnableAWSServiceAccess(servicePrincipal string) error {
	b.mu.Lock("EnableAWSServiceAccess")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	b.serviceAccess.Put(&EnabledServicePrincipal{ServicePrincipal: servicePrincipal, DateEnabled: time.Now()})

	return nil
}

// DisableAWSServiceAccess disables a service principal.
func (b *InMemoryBackend) DisableAWSServiceAccess(servicePrincipal string) error {
	b.mu.Lock("DisableAWSServiceAccess")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	b.serviceAccess.Delete(servicePrincipal)

	return nil
}

// ListAWSServiceAccessForOrganization returns enabled service principals.
func (b *InMemoryBackend) ListAWSServiceAccessForOrganization() ([]EnabledServicePrincipal, error) {
	b.mu.RLock("ListAWSServiceAccessForOrganization")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	out := make([]EnabledServicePrincipal, 0, b.serviceAccess.Len())

	for _, sp := range b.serviceAccess.All() {
		out = append(out, *sp)
	}

	slices.SortFunc(out, func(a, b EnabledServicePrincipal) int {
		return cmp.Compare(a.ServicePrincipal, b.ServicePrincipal)
	})

	return out, nil
}
