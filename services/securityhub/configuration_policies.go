package securityhub

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) configPolicyARN(id string) string {
	return arn.Build("securityhub", b.region, b.accountID, fmt.Sprintf("configuration-policy/%s", id))
}

// clone deep-copies p's map fields. Tags is created aliased to b.tags[Arn]
// (same map object, see CreateConfigurationPolicy), and TagResource/
// UntagResource mutate that map in place under lock -- a shallow "c := *p"
// leaves the returned copy's Tags field pointing at that live, mutable map.
func (p *ConfigurationPolicy) clone() *ConfigurationPolicy {
	c := *p
	c.ConfigurationPolicy = maps.Clone(p.ConfigurationPolicy)
	c.Tags = maps.Clone(p.Tags)

	return &c
}

func (b *InMemoryBackend) CreateConfigurationPolicy(
	name, description string,
	policy map[string]any,
	tags map[string]string,
) (*ConfigurationPolicy, error) {
	b.mu.Lock("CreateConfigurationPolicy")
	defer b.mu.Unlock()

	b.configPolicySeq++
	id := fmt.Sprintf("policy-%d", b.configPolicySeq)
	now := time.Now().UTC().Format(time.RFC3339)

	cp := &ConfigurationPolicy{
		Arn:                 b.configPolicyARN(id),
		Id:                  id,
		Name:                name,
		Description:         description,
		CreatedAt:           now,
		UpdatedAt:           now,
		ConfigurationPolicy: policy,
		Tags:                tags,
	}
	b.configPolicies.Put(cp)

	if len(tags) > 0 {
		b.tags[cp.Arn] = tags
	}

	return cp.clone(), nil
}

func (b *InMemoryBackend) GetConfigurationPolicy(identifier string) (*ConfigurationPolicy, error) {
	b.mu.RLock("GetConfigurationPolicy")
	defer b.mu.RUnlock()

	cp, ok := b.configPolicies.Get(identifier)
	if !ok {
		// also try by ARN
		for _, p := range b.configPolicies.All() {
			if p.Arn == identifier || p.Name == identifier {
				return p.clone(), nil
			}
		}

		return nil, ErrNotFound
	}

	return cp.clone(), nil
}

func (b *InMemoryBackend) UpdateConfigurationPolicy(
	identifier, name, description string,
	policy map[string]any,
) (*ConfigurationPolicy, error) {
	b.mu.Lock("UpdateConfigurationPolicy")
	defer b.mu.Unlock()

	var target *ConfigurationPolicy

	if cp, ok := b.configPolicies.Get(identifier); ok {
		target = cp
	} else {
		for _, p := range b.configPolicies.All() {
			if p.Arn == identifier || p.Name == identifier {
				target = p

				break
			}
		}
	}

	if target == nil {
		return nil, ErrNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)

	if name != "" {
		target.Name = name
	}

	if description != "" {
		target.Description = description
	}

	if policy != nil {
		target.ConfigurationPolicy = policy
	}

	target.UpdatedAt = now

	return target.clone(), nil
}

func (b *InMemoryBackend) DeleteConfigurationPolicy(identifier string) error {
	b.mu.Lock("DeleteConfigurationPolicy")
	defer b.mu.Unlock()

	if b.configPolicies.Delete(identifier) {
		return nil
	}

	for _, p := range b.configPolicies.All() {
		if p.Arn == identifier || p.Name == identifier {
			b.configPolicies.Delete(p.Id)

			return nil
		}
	}

	return ErrNotFound
}

func (b *InMemoryBackend) ListConfigurationPolicies(nextToken string, maxResults int) ([]*ConfigurationPolicy, string) {
	b.mu.RLock("ListConfigurationPolicies")
	defer b.mu.RUnlock()

	snap := b.configPolicies.All()
	all := make([]*ConfigurationPolicy, 0, len(snap))

	for _, p := range snap {
		all = append(all, p.clone())
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) StartConfigurationPolicyAssociation(
	configPolicyIdentifier, targetID, targetType string,
) (*ConfigurationPolicyAssociation, error) {
	b.mu.Lock("StartConfigurationPolicyAssociation")
	defer b.mu.Unlock()

	var policyID string

	for _, p := range b.configPolicies.All() {
		if p.Id == configPolicyIdentifier || p.Arn == configPolicyIdentifier || p.Name == configPolicyIdentifier {
			policyID = p.Id

			break
		}
	}

	if policyID == "" {
		return nil, ErrNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)
	assoc := &ConfigurationPolicyAssociation{
		ConfigurationPolicyId:    policyID,
		TargetId:                 targetID,
		TargetType:               targetType,
		AssociationType:          "APPLIED",
		UpdatedAt:                now,
		AssociationStatus:        "SUCCESS", //nolint:goconst // existing issue.
		AssociationStatusMessage: "",
	}
	b.configPolicyAssocs.Put(assoc)

	return assoc, nil
}

func (b *InMemoryBackend) StartConfigurationPolicyDisassociation(
	configPolicyIdentifier, targetID, targetType string, //nolint:revive // existing issue.
) error {
	b.mu.Lock("StartConfigurationPolicyDisassociation")
	defer b.mu.Unlock()

	b.configPolicyAssocs.Delete(targetID)

	return nil
}

func (b *InMemoryBackend) GetConfigurationPolicyAssociation(
	targetID, targetType string, //nolint:revive // existing issue.
) (*ConfigurationPolicyAssociation, error) {
	b.mu.RLock("GetConfigurationPolicyAssociation")
	defer b.mu.RUnlock()

	assoc, ok := b.configPolicyAssocs.Get(targetID)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *assoc

	return &cp, nil
}

func (b *InMemoryBackend) ListConfigurationPolicyAssociations(
	filterPolicyID, filterType, nextToken string,
	maxResults int,
) ([]*ConfigurationPolicyAssociation, string) {
	b.mu.RLock("ListConfigurationPolicyAssociations")
	defer b.mu.RUnlock()

	var all []*ConfigurationPolicyAssociation

	for _, assoc := range b.configPolicyAssocs.All() {
		if filterPolicyID != "" && assoc.ConfigurationPolicyId != filterPolicyID {
			continue
		}

		if filterType != "" && assoc.AssociationType != filterType {
			continue
		}

		cp := *assoc
		all = append(all, &cp)
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) BatchGetConfigurationPolicyAssociations(
	requests []map[string]any,
) ([]*ConfigurationPolicyAssociation, []map[string]any) {
	b.mu.RLock("BatchGetConfigurationPolicyAssociations")
	defer b.mu.RUnlock()

	var found []*ConfigurationPolicyAssociation
	var unprocessed []map[string]any

	for _, req := range requests {
		target, _ := req["ConfigurationPolicyAssociationIdentifiers"].(map[string]any)
		if target == nil {
			target = req
		}

		targetID, _ := target["TargetId"].(string)
		if targetID == "" {
			targetID, _ = req["TargetId"].(string)
		}

		if assoc, ok := b.configPolicyAssocs.Get(targetID); ok {
			cp := *assoc
			found = append(found, &cp)
		} else {
			unprocessed = append(unprocessed, map[string]any{
				"ConfigurationPolicyAssociationIdentifiers": req,
				"ErrorCode":   errCodeResourceNotFound,
				"ErrorReason": "Association not found",
			})
		}
	}

	return found, unprocessed
}
