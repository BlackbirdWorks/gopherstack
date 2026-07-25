package cleanrooms

import (
	"fmt"
	"maps"
	"slices"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) ctAssociationARN(membershipID, assocID string) string {
	return arn.Build(
		"cleanrooms",
		b.region,
		b.accountID,
		fmt.Sprintf("membership/%s/configuredtableassociation/%s", membershipID, assocID),
	)
}

func (b *InMemoryBackend) CreateConfiguredTableAssociation(
	membershipID, name, description, configuredTableID, roleArn string,
	tags map[string]string,
) (*ConfiguredTableAssociation, error) {
	b.mu.Lock("CreateConfiguredTableAssociation")
	defer b.mu.Unlock()
	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}
	ct, ok := b.configuredTables.Get(configuredTableID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	assoc := &ConfiguredTableAssociation{
		ConfiguredTableAssociationIdentifier: id,
		Arn:                                  b.ctAssociationARN(membershipID, id),
		MembershipIdentifier:                 membershipID,
		MembershipArn:                        mem.Arn,
		ConfiguredTableIdentifier:            configuredTableID,
		ConfiguredTableArn:                   ct.Arn,
		Name:                                 name,
		Description:                          description,
		RoleArn:                              roleArn,
		CreateTime:                           ts,
		UpdateTime:                           ts,
		Tags:                                 tags,
		ID:                                   id,
		MembershipID:                         membershipID,
		ConfiguredTableID:                    configuredTableID,
	}
	b.ctAssociations.Put(assoc)
	if len(tags) > 0 {
		b.tagsByArn[assoc.Arn] = maps.Clone(tags)
	}

	return assoc, nil
}

func (b *InMemoryBackend) GetConfiguredTableAssociation(
	membershipID, assocID string,
) (*ConfiguredTableAssociation, error) {
	b.mu.RLock("GetConfiguredTableAssociation")
	defer b.mu.RUnlock()
	assoc, ok := b.ctAssociations.Get(membershipKey(membershipID, assocID))
	if !ok {
		return nil, ErrNotFound
	}

	return assoc, nil
}

func (b *InMemoryBackend) ListConfiguredTableAssociations(
	membershipID, maxResults, nextToken string,
) ([]*ConfiguredTableAssociationSummary, string, error) {
	b.mu.RLock("ListConfiguredTableAssociations")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}
	var items []*ConfiguredTableAssociationSummary
	for _, a := range b.ctAssociationsByMembership.Get(membershipID) {
		items = append(items, &ConfiguredTableAssociationSummary{
			ConfiguredTableAssociationIdentifier: a.ConfiguredTableAssociationIdentifier,
			Arn:                                  a.Arn,
			MembershipIdentifier:                 a.MembershipIdentifier,
			MembershipArn:                        a.MembershipArn,
			ConfiguredTableIdentifier:            a.ConfiguredTableIdentifier,
			Name:                                 a.Name,
			CreateTime:                           a.CreateTime,
			UpdateTime:                           a.UpdateTime,
			ID:                                   a.ID,
			MembershipID:                         a.MembershipID,
			ConfiguredTableID:                    a.ConfiguredTableID,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].ID < items[j].ID
	})
	page, next := paginate(items, maxResults, nextToken)

	return page, next, nil
}

func (b *InMemoryBackend) UpdateConfiguredTableAssociation(
	membershipID, assocID, description, roleArn string,
) (*ConfiguredTableAssociation, error) {
	b.mu.Lock("UpdateConfiguredTableAssociation")
	defer b.mu.Unlock()
	assoc, ok := b.ctAssociations.Get(membershipKey(membershipID, assocID))
	if !ok {
		return nil, ErrNotFound
	}
	if description != "" {
		assoc.Description = description
	}
	if roleArn != "" {
		assoc.RoleArn = roleArn
	}
	assoc.UpdateTime = b.now()

	return assoc, nil
}

func (b *InMemoryBackend) DeleteConfiguredTableAssociation(membershipID, assocID string) error {
	b.mu.Lock("DeleteConfiguredTableAssociation")
	defer b.mu.Unlock()
	key := membershipKey(membershipID, assocID)
	assoc, ok := b.ctAssociations.Get(key)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, assoc.Arn)
	b.ctAssociations.Delete(key)

	for _, rule := range slices.Clone(b.ctaAnalysisRulesByAssociation.Get(assocID)) {
		b.ctaAnalysisRules.Delete(ctaAnalysisRuleKey(rule.ConfiguredTableAssociationIdentifier, rule.Type))
	}

	return nil
}

func (b *InMemoryBackend) CreateConfiguredTableAssociationAnalysisRule(
	membershipID, assocID, ruleType string,
	policy map[string]any,
) (*ConfiguredTableAssociationAnalysisRule, error) {
	b.mu.Lock("CreateConfiguredTableAssociationAnalysisRule")
	defer b.mu.Unlock()
	assoc, ok := b.ctAssociations.Get(membershipKey(membershipID, assocID))
	if !ok {
		return nil, ErrNotFound
	}
	if b.ctaAnalysisRules.Has(ctaAnalysisRuleKey(assocID, ruleType)) {
		return nil, ErrAlreadyExists
	}
	mem, _ := b.memberships.Get(membershipID)
	ts := b.now()
	rule := &ConfiguredTableAssociationAnalysisRule{
		ConfiguredTableAssociationIdentifier: assocID,
		ConfiguredTableAssociationArn:        assoc.Arn,
		MembershipIdentifier:                 membershipID,
		MembershipArn:                        mem.Arn,
		Type:                                 ruleType,
		Policy:                               policy,
		CreateTime:                           ts,
		UpdateTime:                           ts,
	}
	b.ctaAnalysisRules.Put(rule)
	if !contains(assoc.AnalysisRuleTypes, ruleType) {
		assoc.AnalysisRuleTypes = append(assoc.AnalysisRuleTypes, ruleType)
	}

	return rule, nil
}

func (b *InMemoryBackend) GetConfiguredTableAssociationAnalysisRule(
	_, assocID, ruleType string,
) (*ConfiguredTableAssociationAnalysisRule, error) {
	b.mu.RLock("GetConfiguredTableAssociationAnalysisRule")
	defer b.mu.RUnlock()
	rule, ok := b.ctaAnalysisRules.Get(ctaAnalysisRuleKey(assocID, ruleType))
	if !ok {
		return nil, ErrNotFound
	}

	return rule, nil
}

func (b *InMemoryBackend) UpdateConfiguredTableAssociationAnalysisRule(
	_, assocID, ruleType string,
	policy map[string]any,
) (*ConfiguredTableAssociationAnalysisRule, error) {
	b.mu.Lock("UpdateConfiguredTableAssociationAnalysisRule")
	defer b.mu.Unlock()
	rule, ok := b.ctaAnalysisRules.Get(ctaAnalysisRuleKey(assocID, ruleType))
	if !ok {
		return nil, ErrNotFound
	}
	rule.Policy = policy
	rule.UpdateTime = b.now()

	return rule, nil
}

func (b *InMemoryBackend) DeleteConfiguredTableAssociationAnalysisRule(
	membershipID, assocID, ruleType string,
) error {
	b.mu.Lock("DeleteConfiguredTableAssociationAnalysisRule")
	defer b.mu.Unlock()
	if !b.ctaAnalysisRules.Delete(ctaAnalysisRuleKey(assocID, ruleType)) {
		return ErrNotFound
	}
	if assoc, assocOK := b.ctAssociations.Get(membershipKey(membershipID, assocID)); assocOK {
		assoc.AnalysisRuleTypes = removeFrom(assoc.AnalysisRuleTypes, ruleType)
	}

	return nil
}
