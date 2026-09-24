package eks

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// identityProviderTransitionDelay is the async delay before a CREATING identity
// provider config reaches ACTIVE, matching the 100ms scale used by
// clusterTransitionDelay/addonTransitionDelay/nodegroupTransitionDelay.
const identityProviderTransitionDelay = 100 * time.Millisecond

// AssociateIdentityProviderConfig associates an identity provider configuration with a cluster.
// It also creates and stores an Update record (transitioning InProgress ->
// Successful like every other async EKS op) so a caller polling DescribeUpdate
// for the returned ID finds it; previously no Update was ever stored, so a
// real client polling on the fabricated ID it got back always saw
// ResourceNotFoundException.
func (b *InMemoryBackend) AssociateIdentityProviderConfig(
	clusterName, configType, name string,
	params, requiredClaims, kv map[string]string,
) (*IdentityProviderConfig, *Update, error) {
	b.mu.Lock("AssociateIdentityProviderConfig")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if _, ok := b.identityProviderConfigs.Get(identityProviderConfigKey(clusterName, name)); ok {
		return nil, nil, fmt.Errorf(
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

	idpResource := "identityproviderconfig/" + clusterName + "/oidc/" + name + "/" + stableID(clusterName+"/"+name)
	idpARN := arn.Build("eks", b.region, b.accountID, idpResource)

	cfg := &IdentityProviderConfig{
		ClusterName:    clusterName,
		Name:           name,
		ARN:            idpARN,
		Type:           configType,
		Status:         statusCreating,
		OIDC:           params,
		RequiredClaims: requiredClaims,
		CreatedAt:      time.Now().UTC(),
		Tags:           t,
	}
	b.identityProviderConfigs.Put(cfg)

	// Schedule async transition CREATING -> ACTIVE, mirroring
	// scheduleClusterActivation/CreateAddon/CreateNodegroup. Previously nothing
	// in this backend ever advanced Status past CREATING -- no ticker, no
	// later call -- while sibling cluster/addon/nodegroup resources in this
	// same service transition correctly.
	b.work.After("IdentityProviderTransition", identityProviderTransitionDelay, func() {
		b.mu.Lock("AssociateIdentityProviderConfig-async")
		defer b.mu.Unlock()

		if c, found := b.identityProviderConfigs.Get(identityProviderConfigKey(clusterName, name)); found &&
			c.Status == statusCreating {
			c.Status = statusActive
		}
	})

	u := &Update{
		ID:          stableID(clusterName + "/idp-update/" + name + "/" + time.Now().String()),
		ClusterName: clusterName,
		Status:      statusInProgress,
		Type:        opAssociateIdentityProviderConfig,
		Params:      []UpdateParam{{Type: "IdentityProviderConfig", Value: name}},
		CreatedAt:   time.Now().UTC(),
	}
	b.storeUpdateLocked(u)
	b.scheduleUpdateTransition(clusterName, u.ID)

	cp := *cfg

	return &cp, u.clone(), nil
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
func (b *InMemoryBackend) DisassociateIdentityProviderConfig(clusterName, name string) (*Update, error) {
	b.mu.Lock("DisassociateIdentityProviderConfig")
	defer b.mu.Unlock()

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

	if cfg.Tags != nil {
		cfg.Tags.Close()
	}

	b.identityProviderConfigs.Delete(identityProviderConfigKey(clusterName, name))

	u := &Update{
		ID:          stableID(clusterName + "/idp-disassociate/" + name + "/" + time.Now().String()),
		ClusterName: clusterName,
		Status:      statusInProgress,
		Type:        opDisassociateIdentityProviderConfig,
		Params:      []UpdateParam{{Type: "IdentityProviderConfig", Value: name}},
		CreatedAt:   time.Now().UTC(),
	}
	b.storeUpdateLocked(u)
	b.scheduleUpdateTransition(clusterName, u.ID)

	return u.clone(), nil
}
