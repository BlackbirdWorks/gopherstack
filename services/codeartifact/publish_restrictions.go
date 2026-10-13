package codeartifact

import (
	"fmt"
	"slices"
)

// checkPublishAllowed rejects a new version when the package's own PUBLISH origin control or the
// effective PUBLISH restriction of its package group blocks it (BLOCK wins over ALLOW). Callers hold b.mu.
func (b *InMemoryBackend) checkPublishAllowed(region, domainName, repoName, format, namespace, name string) error {
	if pkg, ok := b.packages.Get(regionKey(region, packageKey(domainName, repoName, format, namespace, name))); ok &&
		pkg.OriginConfigPublish == restrictionModeBlock {
		return fmt.Errorf("%w: The provided package is configured to block new version publishes", ErrValidation)
	}

	entries := b.packageGroupsByRegion.Get(region)

	match, _ := bestMatchingGroup(entries, domainName, format, namespace, name)
	if match == nil {
		return nil
	}

	domainGroups := make([]*PackageGroup, 0, len(entries))

	for _, g := range entries {
		if g.DomainName == domainName {
			domainGroups = append(domainGroups, g)
		}
	}

	eff := resolveEffectiveRestriction(domainGroups, match, restrictionTypePublish)

	switch eff.EffectiveMode {
	case restrictionModeBlock:
		return fmt.Errorf("%w: package group %s blocks publishing package versions", ErrValidation, match.Pattern)
	case restrictionModeAllowSpecificRepo:
		owner := match
		if eff.InheritedFrom != nil {
			owner = eff.InheritedFrom
		}

		if r := owner.Restrictions[restrictionTypePublish]; r == nil ||
			!slices.Contains(r.AllowedRepositories, repoName) {
			return fmt.Errorf(
				"%w: package group %s does not allow publishing to repository %s",
				ErrValidation, owner.Pattern, repoName,
			)
		}
	}

	return nil
}
