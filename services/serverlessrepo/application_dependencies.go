package serverlessrepo

import (
	"fmt"
	"sort"
	"strings"
)

// AddApplicationDependencyInternal seeds a nested dependency for a version.
func (b *InMemoryBackend) AddApplicationDependencyInternal(
	appName, semanticVersion string,
	dependency ApplicationDependency,
) error {
	b.mu.Lock("AddApplicationDependencyInternal")
	defer b.mu.Unlock()

	if !b.applications.Has(appName) {
		return fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	if b.appDependencies[appName] == nil {
		b.appDependencies[appName] = make(map[string][]*ApplicationDependency)
	}

	dep := dependency
	b.appDependencies[appName][semanticVersion] = append(b.appDependencies[appName][semanticVersion], &dep)

	return nil
}

// ListApplicationDependencies returns nested application dependencies for an application version.
func (b *InMemoryBackend) ListApplicationDependencies(
	appName, semanticVersion string,
) ([]*ApplicationDependency, error) {
	b.mu.RLock("ListApplicationDependencies")
	defer b.mu.RUnlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	deps := make([]*ApplicationDependency, 0)
	b.collectDependencies(appName, semanticVersion, make(map[string]struct{}), &deps)

	sort.Slice(deps, func(i, j int) bool {
		if deps[i].ApplicationID != deps[j].ApplicationID {
			return deps[i].ApplicationID < deps[j].ApplicationID
		}

		return deps[i].SemanticVersion < deps[j].SemanticVersion
	})

	return deps, nil
}

func (b *InMemoryBackend) collectDependencies(
	appName, semanticVersion string,
	seen map[string]struct{},
	deps *[]*ApplicationDependency,
) {
	for _, dependency := range b.appDependencies[appName][semanticVersion] {
		key := dependency.ApplicationID + "@" + dependency.SemanticVersion
		if _, ok := seen[key]; ok {
			continue
		}

		seen[key] = struct{}{}
		cp := *dependency
		*deps = append(*deps, &cp)

		if separator := strings.LastIndex(dependency.ApplicationID, "/"); separator >= 0 {
			b.collectDependencies(dependency.ApplicationID[separator+1:], dependency.SemanticVersion, seen, deps)
		}
	}
}
