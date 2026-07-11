package quicksight

import (
	"sort"
)

// storedIdentityPropagationConfig is the persisted representation of one
// service's authorized identity-propagation targets, keyed by Service.
type storedIdentityPropagationConfig struct {
	Service           string   `json:"service"`
	AuthorizedTargets []string `json:"authorizedTargets,omitempty"`
}

func (c *storedIdentityPropagationConfig) toIdentityPropagationConfig() *IdentityPropagationConfig {
	targets := make([]string, len(c.AuthorizedTargets))
	copy(targets, c.AuthorizedTargets)

	return &IdentityPropagationConfig{
		Service:           c.Service,
		AuthorizedTargets: targets,
	}
}

func isValidServiceType(service string) bool {
	switch service {
	case "REDSHIFT", "QBUSINESS", "ATHENA":
		return true
	}

	return false
}

// ---- Identity propagation configuration ----

func (b *InMemoryBackend) UpdateIdentityPropagationConfig(_, service string, authorizedTargets []string) error {
	if !isValidServiceType(service) {
		return ErrValidation
	}

	b.mu.Lock("UpdateIdentityPropagationConfig")
	defer b.mu.Unlock()

	b.identityPropagationConfigs.Put(&storedIdentityPropagationConfig{
		Service:           service,
		AuthorizedTargets: authorizedTargets,
	})

	return nil
}

func (b *InMemoryBackend) DeleteIdentityPropagationConfig(accountID, service string) error {
	b.mu.Lock("DeleteIdentityPropagationConfig")
	defer b.mu.Unlock()

	key := identityPropagationKey(accountID, service)
	if !b.identityPropagationConfigs.Delete(key) {
		return ErrIdentityPropagationConfigNotFound
	}

	return nil
}

func (b *InMemoryBackend) ListIdentityPropagationConfigs(_ string) ([]*IdentityPropagationConfig, error) {
	b.mu.RLock("ListIdentityPropagationConfigs")
	defer b.mu.RUnlock()

	all := b.identityPropagationConfigs.All()
	sort.Slice(all, func(i, j int) bool { return all[i].Service < all[j].Service })

	result := make([]*IdentityPropagationConfig, 0, len(all))
	for _, cfg := range all {
		result = append(result, cfg.toIdentityPropagationConfig())
	}

	return result, nil
}
