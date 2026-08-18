package s3control

import (
	"fmt"
	"sort"
)

// CreateAccessPointForObjectLambda creates an Object Lambda access point.
func (b *InMemoryBackend) CreateAccessPointForObjectLambda(accountID, name string) *ObjectLambdaAccessPoint {
	b.mu.Lock("CreateAccessPointForObjectLambda")
	defer b.mu.Unlock()

	arn := fmt.Sprintf(arnFmtObjectLambda, b.region, accountID, name)

	const (
		maxAliasLen = 63
		aliasSuffix = "--ol-s3"
	)

	aliasPrefix := accountID
	if len(aliasPrefix) > aliasAccountIDMaxLen {
		aliasPrefix = aliasPrefix[:aliasAccountIDMaxLen]
	}

	alias := fmt.Sprintf("%s-%s%s", name, aliasPrefix, aliasSuffix)
	if len(alias) > maxAliasLen {
		alias = alias[:maxAliasLen]
	}

	ap := &ObjectLambdaAccessPoint{
		AccountID:                  accountID,
		Name:                       name,
		ObjectLambdaAccessPointArn: arn,
		Alias: &ObjectLambdaAccessPointAlias{
			Value:  alias,
			Status: "READY",
		},
	}
	b.objectLambdaAccessPoints.Put(ap)

	return cloneObjectLambdaAccessPoint(ap)
}

// ---- Object Lambda Access Points ----

// GetAccessPointForObjectLambda returns an Object Lambda access point.
func (b *InMemoryBackend) GetAccessPointForObjectLambda(
	accountID, name string,
) (*ObjectLambdaAccessPoint, error) {
	b.mu.RLock("GetAccessPointForObjectLambda")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	ap, ok := b.objectLambdaAccessPoints.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errObjectLambdaAPNotFound, name)
	}

	return cloneObjectLambdaAccessPoint(ap), nil
}

// DeleteAccessPointForObjectLambda removes an Object Lambda access point and
// cascade-cleans its policy, configuration, and generic resource tags so a
// delete/recreate cycle under the same name never resurfaces stale state.
func (b *InMemoryBackend) DeleteAccessPointForObjectLambda(accountID, name string) error {
	b.mu.Lock("DeleteAccessPointForObjectLambda")
	defer b.mu.Unlock()

	key := accountID + ":" + name

	ap, ok := b.objectLambdaAccessPoints.Get(key)
	if !ok {
		return fmt.Errorf("%w: %s", errObjectLambdaAPNotFound, name)
	}

	arn := ap.ObjectLambdaAccessPointArn

	b.objectLambdaAccessPoints.Delete(key)
	delete(b.objectLambdaAPPolicies, key)
	delete(b.objectLambdaAPConfigs, key)
	delete(b.resourceTags, arn)

	return nil
}

// ListAccessPointsForObjectLambda lists Object Lambda access points for an account.
func (b *InMemoryBackend) ListAccessPointsForObjectLambda(
	accountID string,
) []*ObjectLambdaAccessPoint {
	b.mu.RLock("ListAccessPointsForObjectLambda")
	defer b.mu.RUnlock()

	var out []*ObjectLambdaAccessPoint
	for _, ap := range b.objectLambdaAccessPoints.All() {
		if ap.AccountID == accountID {
			out = append(out, cloneObjectLambdaAccessPoint(ap))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// GetAccessPointPolicyForObjectLambda returns the policy for an Object Lambda AP.
func (b *InMemoryBackend) GetAccessPointPolicyForObjectLambda(
	accountID, name string,
) (string, error) {
	b.mu.RLock("GetAccessPointPolicyForObjectLambda")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	if !b.objectLambdaAccessPoints.Has(key) {
		return "", fmt.Errorf("%w: %s", errObjectLambdaAPNotFound, name)
	}

	return b.objectLambdaAPPolicies[key], nil
}

// PutAccessPointPolicyForObjectLambda sets the policy for an Object Lambda AP.
func (b *InMemoryBackend) PutAccessPointPolicyForObjectLambda(
	accountID, name, policy string,
) error {
	b.mu.Lock("PutAccessPointPolicyForObjectLambda")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if !b.objectLambdaAccessPoints.Has(key) {
		return fmt.Errorf("%w: %s", errObjectLambdaAPNotFound, name)
	}
	b.objectLambdaAPPolicies[key] = policy

	return nil
}

// DeleteAccessPointPolicyForObjectLambda removes the policy from an Object Lambda AP.
func (b *InMemoryBackend) DeleteAccessPointPolicyForObjectLambda(accountID, name string) error {
	b.mu.Lock("DeleteAccessPointPolicyForObjectLambda")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	delete(b.objectLambdaAPPolicies, key)

	return nil
}

// GetAccessPointPolicyStatusForObjectLambda returns the policy status for an Object Lambda AP.
func (b *InMemoryBackend) GetAccessPointPolicyStatusForObjectLambda(
	accountID, name string,
) (bool, error) {
	b.mu.RLock("GetAccessPointPolicyStatusForObjectLambda")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	if !b.objectLambdaAccessPoints.Has(key) {
		return false, fmt.Errorf("%w: %s", errObjectLambdaAPNotFound, name)
	}

	return b.objectLambdaAPPolicies[key] != "", nil
}

// GetAccessPointConfigurationForObjectLambda returns the configuration for an Object Lambda AP.
func (b *InMemoryBackend) GetAccessPointConfigurationForObjectLambda(
	accountID, name string,
) (string, error) {
	b.mu.RLock("GetAccessPointConfigurationForObjectLambda")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	if !b.objectLambdaAccessPoints.Has(key) {
		return "", fmt.Errorf("%w: %s", errObjectLambdaAPNotFound, name)
	}

	return b.objectLambdaAPConfigs[key], nil
}

// PutAccessPointConfigurationForObjectLambda sets the configuration for an Object Lambda AP.
func (b *InMemoryBackend) PutAccessPointConfigurationForObjectLambda(
	accountID, name, config string,
) error {
	b.mu.Lock("PutAccessPointConfigurationForObjectLambda")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if !b.objectLambdaAccessPoints.Has(key) {
		return fmt.Errorf("%w: %s", errObjectLambdaAPNotFound, name)
	}
	b.objectLambdaAPConfigs[key] = config

	return nil
}
