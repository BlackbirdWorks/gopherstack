package cleanrooms

import (
	"fmt"
	"maps"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) camaARN(membershipID, id string) string {
	return arn.Build(
		"cleanrooms",
		b.region,
		b.accountID,
		fmt.Sprintf("membership/%s/configuredaudiencemodelassociation/%s", membershipID, id),
	)
}

func toConfiguredAudienceModelAssociationSummary(
	a *ConfiguredAudienceModelAssociation,
) *ConfiguredAudienceModelAssociationSummary {
	return &ConfiguredAudienceModelAssociationSummary{
		ConfiguredAudienceModelAssociationIdentifier: a.ConfiguredAudienceModelAssociationIdentifier,
		Arn:                     a.Arn,
		CollaborationArn:        a.CollaborationArn,
		CollaborationIdentifier: a.CollaborationIdentifier,
		MembershipArn:           a.MembershipArn,
		MembershipIdentifier:    a.MembershipIdentifier,
		Name:                    a.Name,
		CreateTime:              a.CreateTime,
		UpdateTime:              a.UpdateTime,
		ID:                      a.ID,
		MembershipID:            a.MembershipID,
		CollaborationID:         a.CollaborationID,
	}
}

func (b *InMemoryBackend) CreateConfiguredAudienceModelAssociation(
	membershipID, configuredAudienceModelArn, name, description string,
	manageResourcePolicies bool,
	tags map[string]string,
) (*ConfiguredAudienceModelAssociation, error) {
	if configuredAudienceModelArn == "" || name == "" {
		return nil, ErrValidation
	}
	b.mu.Lock("CreateConfiguredAudienceModelAssociation")
	defer b.mu.Unlock()
	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	collab, _ := b.collaborations.Get(mem.CollaborationID)
	var collabArn string
	if collab != nil {
		collabArn = collab.Arn
	}
	assoc := &ConfiguredAudienceModelAssociation{
		ConfiguredAudienceModelAssociationIdentifier: id,
		Arn:                        b.camaARN(membershipID, id),
		CollaborationArn:           collabArn,
		CollaborationIdentifier:    mem.CollaborationID,
		MembershipArn:              mem.Arn,
		MembershipIdentifier:       membershipID,
		ConfiguredAudienceModelArn: configuredAudienceModelArn,
		Name:                       name,
		Description:                description,
		ManageResourcePolicies:     manageResourcePolicies,
		CreateTime:                 ts,
		UpdateTime:                 ts,
		Tags:                       tags,
		ID:                         id,
		MembershipID:               membershipID,
		CollaborationID:            mem.CollaborationID,
	}
	b.camaAssociations.Put(assoc)
	if len(tags) > 0 {
		b.tagsByArn[assoc.Arn] = maps.Clone(tags)
	}

	return assoc, nil
}

func (b *InMemoryBackend) GetConfiguredAudienceModelAssociation(
	membershipID, assocID string,
) (*ConfiguredAudienceModelAssociation, error) {
	b.mu.RLock("GetConfiguredAudienceModelAssociation")
	defer b.mu.RUnlock()
	assoc, ok := b.camaAssociations.Get(membershipKey(membershipID, assocID))
	if !ok {
		return nil, ErrNotFound
	}

	return assoc, nil
}

func (b *InMemoryBackend) ListConfiguredAudienceModelAssociations(
	membershipID, maxResults, nextToken string,
) ([]*ConfiguredAudienceModelAssociationSummary, string, error) {
	b.mu.RLock("ListConfiguredAudienceModelAssociations")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listItems(
		b.camaAssociationsByMembership.Get(membershipID),
		nil,
		toConfiguredAudienceModelAssociationSummary,
		func(a, c *ConfiguredAudienceModelAssociationSummary) bool {
			return a.ID < c.ID
		},
		maxResults, nextToken,
	)

	return page, next, nil
}

func (b *InMemoryBackend) UpdateConfiguredAudienceModelAssociation(
	membershipID, assocID, name, description string,
) (*ConfiguredAudienceModelAssociation, error) {
	b.mu.Lock("UpdateConfiguredAudienceModelAssociation")
	defer b.mu.Unlock()
	assoc, ok := b.camaAssociations.Get(membershipKey(membershipID, assocID))
	if !ok {
		return nil, ErrNotFound
	}
	if name != "" {
		assoc.Name = name
	}
	if description != "" {
		assoc.Description = description
	}
	assoc.UpdateTime = b.now()

	return assoc, nil
}

func (b *InMemoryBackend) DeleteConfiguredAudienceModelAssociation(
	membershipID, assocID string,
) error {
	b.mu.Lock("DeleteConfiguredAudienceModelAssociation")
	defer b.mu.Unlock()
	key := membershipKey(membershipID, assocID)
	assoc, ok := b.camaAssociations.Get(key)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, assoc.Arn)
	b.camaAssociations.Delete(key)

	return nil
}

func (b *InMemoryBackend) GetCollaborationConfiguredAudienceModelAssociation(
	collaborationID, assocID string,
) (*ConfiguredAudienceModelAssociation, error) {
	b.mu.RLock("GetCollaborationConfiguredAudienceModelAssociation")
	defer b.mu.RUnlock()
	var found *ConfiguredAudienceModelAssociation
	b.camaAssociations.Range(func(a *ConfiguredAudienceModelAssociation) bool {
		if a.CollaborationID == collaborationID && a.ID == assocID {
			found = a

			return false
		}

		return true
	})
	if found == nil {
		return nil, ErrNotFound
	}

	return found, nil
}

func (b *InMemoryBackend) ListCollaborationConfiguredAudienceModelAssociations(
	collaborationID, maxResults, nextToken string,
) ([]*ConfiguredAudienceModelAssociationSummary, string, error) {
	b.mu.RLock("ListCollaborationConfiguredAudienceModelAssociations")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listNestedItems(
		b.camaAssociations.All(),
		func(a *ConfiguredAudienceModelAssociation) bool {
			return a.CollaborationID == collaborationID
		},
		toConfiguredAudienceModelAssociationSummary,
		func(a, c *ConfiguredAudienceModelAssociationSummary) bool {
			return a.ID < c.ID
		},
		maxResults, nextToken,
	)

	return page, next, nil
}
