package eks

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// AssociateIdentityProviderConfig associates an identity provider configuration with a cluster.
func (b *InMemoryBackend) AssociateIdentityProviderConfig(
	clusterName, configType, name string,
	params, kv map[string]string,
) (*IdentityProviderConfig, error) {
	b.mu.Lock("AssociateIdentityProviderConfig")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if _, ok := b.identityProviderConfigs.Get(identityProviderConfigKey(clusterName, name)); ok {
		return nil, fmt.Errorf(
			"%w: identity provider config %s already exists in cluster %s",
			ErrAlreadyExists,
			name,
			clusterName,
		)
	}

	t := tags.New("eks.idp." + clusterName + "." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	cfg := &IdentityProviderConfig{
		ClusterName: clusterName,
		Name:        name,
		Type:        configType,
		Status:      statusCreating,
		OIDC:        params,
		CreatedAt:   time.Now().UTC(),
		Tags:        t,
	}
	b.identityProviderConfigs.Put(cfg)
	cp := *cfg

	return &cp, nil
}

// DescribeIdentityProviderConfig returns an identity provider config by name.
func (b *InMemoryBackend) DescribeIdentityProviderConfig(clusterName, name string) (*IdentityProviderConfig, error) {
	b.mu.RLock("DescribeIdentityProviderConfig")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	cfg, ok := b.identityProviderConfigs.Get(identityProviderConfigKey(clusterName, name))
	if !ok {
		return nil, fmt.Errorf(
			"%w: identity provider config %s not found in cluster %s",
			ErrNotFound,
			name,
			clusterName,
		)
	}

	cp := *cfg

	return &cp, nil
}

// ListIdentityProviderConfigs returns all identity provider config summaries for a cluster.
func (b *InMemoryBackend) ListIdentityProviderConfigs(clusterName string) ([]map[string]string, error) {
	b.mu.RLock("ListIdentityProviderConfigs")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	configs := b.identityProviderConfigsByCluster.Get(clusterName)
	result := make([]map[string]string, 0, len(configs))

	for _, cfg := range configs {
		result = append(result, map[string]string{
			keyName: cfg.Name,
			keyType: cfg.Type,
		})
	}

	sort.Slice(result, func(i, j int) bool { return result[i]["name"] < result[j]["name"] })

	return result, nil
}

// DisassociateIdentityProviderConfig removes an identity provider config from a cluster.
func (b *InMemoryBackend) DisassociateIdentityProviderConfig(clusterName, name string) error {
	b.mu.Lock("DisassociateIdentityProviderConfig")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	cfg, ok := b.identityProviderConfigs.Get(identityProviderConfigKey(clusterName, name))
	if !ok {
		return fmt.Errorf("%w: identity provider config %s not found in cluster %s", ErrNotFound, name, clusterName)
	}

	if cfg.Tags != nil {
		cfg.Tags.Close()
	}

	b.identityProviderConfigs.Delete(identityProviderConfigKey(clusterName, name))

	return nil
}
