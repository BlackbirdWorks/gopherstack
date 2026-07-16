package glue

import (
	"fmt"
	"sort"
	"time"
)

func cloneSecurityConfig(sc *SecurityConfiguration) *SecurityConfiguration {
	cp := *sc

	return &cp
}

func (b *InMemoryBackend) CreateSecurityConfiguration(
	name string,
	enc EncryptionConfiguration,
) (*SecurityConfiguration, error) {
	b.mu.Lock("CreateSecurityConfiguration")
	defer b.mu.Unlock()

	if b.securityConfigs.Has(name) {
		return nil, fmt.Errorf(
			"security configuration %q already exists: %w",
			name,
			ErrAlreadyExists,
		)
	}
	sc := &SecurityConfiguration{
		Name:                    name,
		EncryptionConfiguration: enc,
		CreatedTimeStamp:        float64(time.Now().Unix()),
	}
	b.securityConfigs.Put(sc)

	return cloneSecurityConfig(sc), nil
}

func (b *InMemoryBackend) GetSecurityConfiguration(name string) (*SecurityConfiguration, error) {
	b.mu.RLock("GetSecurityConfiguration")
	defer b.mu.RUnlock()

	sc, ok := b.securityConfigs.Get(name)
	if !ok {
		return nil, fmt.Errorf("security configuration %q not found: %w", name, ErrNotFound)
	}

	return cloneSecurityConfig(sc), nil
}

func (b *InMemoryBackend) DeleteSecurityConfiguration(name string) error {
	b.mu.Lock("DeleteSecurityConfiguration")
	defer b.mu.Unlock()

	if !b.securityConfigs.Has(name) {
		return fmt.Errorf("security configuration %q not found: %w", name, ErrNotFound)
	}
	b.securityConfigs.Delete(name)

	return nil
}

func (b *InMemoryBackend) ListSecurityConfigurations() []*SecurityConfiguration {
	b.mu.RLock("ListSecurityConfigurations")
	defer b.mu.RUnlock()

	src := b.securityConfigs.All()
	out := make([]*SecurityConfiguration, 0, len(src))
	for _, sc := range src {
		out = append(out, cloneSecurityConfig(sc))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}
