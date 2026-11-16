package azurearm

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const acrSKUPremium = "Premium"

// ContainerRegistryRetention reports the untagged-manifest retention policy of the registry named name: whether
// it is enabled on a Premium registry, and its period in days.
func (r *Registry) ContainerRegistryRetention(name string) (int, bool) {
	p, ok := r.ProviderNamed(namespaceMicrosoftContainerRegistry)
	if !ok {
		return 0, false
	}

	cp, ok := p.(*ContainerRegistryProvider)
	if !ok {
		return 0, false
	}

	cp.mu.RLock("ContainerRegistryRetention")
	defer cp.mu.RUnlock()

	for _, rec := range cp.res {
		if rec.kind != acrKindRegistry || !strs.Equal(rec.name, name) {
			continue
		}

		if !strings.EqualFold(rec.sku, acrSKUPremium) {
			return 0, false
		}

		policy := mapOf(mapOf(rec.extra["policies"])["retentionPolicy"])
		days := acrRetentionDays

		switch v := policy["days"].(type) {
		case float64:
			days = int(v)
		case int:
			days = v
		case int64:
			days = int(v)
		}

		return days, strings.EqualFold(stringField(policy, acrPropStatus), acrTokenEnabled)
	}

	return 0, false
}
