package securityhub

import (
	"fmt"
	"time"
)

// ConfigurationPolicy represents a Security Hub central configuration policy.
type ConfigurationPolicy struct {
	ConfigurationPolicy map[string]any    `json:"ConfigurationPolicy"`
	Tags                map[string]string `json:"Tags"`
	Arn                 string            `json:"Arn"`
	Id                  string            `json:"Id"` //nolint:revive,staticcheck // existing issue.
	Name                string            `json:"Name"`
	Description         string            `json:"Description"`
	CreatedAt           string            `json:"CreatedAt"`
	UpdatedAt           string            `json:"UpdatedAt"`
}

// ConfigurationPolicyAssociation represents an association between a policy and a target.
type ConfigurationPolicyAssociation struct {
	ConfigurationPolicyId    string `json:"ConfigurationPolicyId"` //nolint:revive,staticcheck // existing issue.
	TargetId                 string `json:"TargetId"`              //nolint:revive,staticcheck // existing issue.
	TargetType               string `json:"TargetType"`
	AssociationType          string `json:"AssociationType"`
	UpdatedAt                string `json:"UpdatedAt"`
	AssociationStatus        string `json:"AssociationStatus"`
	AssociationStatusMessage string `json:"AssociationStatusMessage"`
}

func (b *InMemoryBackend) configPolicyARN(id string) string {
	return fmt.Sprintf("arn:aws:securityhub:%s:%s:configuration-policy/%s", b.region, b.accountID, id)
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
	b.configPolicies[id] = cp

	return cp, nil
}

func (b *InMemoryBackend) GetConfigurationPolicy(identifier string) (*ConfigurationPolicy, error) {
	b.mu.RLock("GetConfigurationPolicy")
	defer b.mu.RUnlock()

	cp, ok := b.configPolicies[identifier]
	if !ok {
		// also try by ARN
		for _, p := range b.configPolicies {
			if p.Arn == identifier || p.Name == identifier {
				c := *p

				return &c, nil
			}
		}

		return nil, ErrNotFound
	}

	c := *cp

	return &c, nil
}

func (b *InMemoryBackend) UpdateConfigurationPolicy(
	identifier, name, description string,
	policy map[string]any,
) (*ConfigurationPolicy, error) {
	b.mu.Lock("UpdateConfigurationPolicy")
	defer b.mu.Unlock()

	var target *ConfigurationPolicy

	if cp, ok := b.configPolicies[identifier]; ok {
		target = cp
	} else {
		for _, p := range b.configPolicies {
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
	c := *target

	return &c, nil
}

func (b *InMemoryBackend) DeleteConfigurationPolicy(identifier string) error {
	b.mu.Lock("DeleteConfigurationPolicy")
	defer b.mu.Unlock()

	if _, ok := b.configPolicies[identifier]; ok {
		delete(b.configPolicies, identifier)

		return nil
	}

	for id, p := range b.configPolicies {
		if p.Arn == identifier || p.Name == identifier {
			delete(b.configPolicies, id)

			return nil
		}
	}

	return ErrNotFound
}

func (b *InMemoryBackend) ListConfigurationPolicies(nextToken string, maxResults int) ([]*ConfigurationPolicy, string) {
	b.mu.RLock("ListConfigurationPolicies")
	defer b.mu.RUnlock()

	var all []*ConfigurationPolicy //nolint:prealloc // existing issue.

	for _, p := range b.configPolicies {
		cp := *p
		all = append(all, &cp)
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) StartConfigurationPolicyAssociation(
	configPolicyIdentifier, targetID, targetType string,
) (*ConfigurationPolicyAssociation, error) {
	b.mu.Lock("StartConfigurationPolicyAssociation")
	defer b.mu.Unlock()

	var policyID string

	for id, p := range b.configPolicies {
		if id == configPolicyIdentifier || p.Arn == configPolicyIdentifier || p.Name == configPolicyIdentifier {
			policyID = id

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
	b.configPolicyAssocs[targetID] = assoc

	return assoc, nil
}

func (b *InMemoryBackend) StartConfigurationPolicyDisassociation(
	configPolicyIdentifier, targetID, targetType string, //nolint:revive // existing issue.
) error {
	b.mu.Lock("StartConfigurationPolicyDisassociation")
	defer b.mu.Unlock()

	delete(b.configPolicyAssocs, targetID)

	return nil
}

func (b *InMemoryBackend) GetConfigurationPolicyAssociation(
	targetID, targetType string, //nolint:revive // existing issue.
) (*ConfigurationPolicyAssociation, error) {
	b.mu.RLock("GetConfigurationPolicyAssociation")
	defer b.mu.RUnlock()

	assoc, ok := b.configPolicyAssocs[targetID]
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

	for _, assoc := range b.configPolicyAssocs {
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

		if assoc, ok := b.configPolicyAssocs[targetID]; ok {
			cp := *assoc
			found = append(found, &cp)
		} else {
			unprocessed = append(unprocessed, map[string]any{
				"ConfigurationPolicyAssociationIdentifiers": req,
				"ErrorCode":   "ResourceNotFoundException", //nolint:goconst // existing issue.
				"ErrorReason": "Association not found",
			})
		}
	}

	return found, unprocessed
}
