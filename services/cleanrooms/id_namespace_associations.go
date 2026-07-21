package cleanrooms

import (
	"fmt"
	"maps"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) idNamespaceAssocARN(membershipID, id string) string {
	return arn.Build(
		"cleanrooms",
		b.region,
		b.accountID,
		fmt.Sprintf("membership/%s/idnamespaceassociation/%s", membershipID, id),
	)
}

func toIDNamespaceAssociationSummary(a *IDNamespaceAssociation) *IDNamespaceAssociationSummary {
	return &IDNamespaceAssociationSummary{
		IDNamespaceAssociationIdentifier: a.IDNamespaceAssociationIdentifier,
		Arn:                              a.Arn,
		CollaborationArn:                 a.CollaborationArn,
		CollaborationIdentifier:          a.CollaborationIdentifier,
		MembershipArn:                    a.MembershipArn,
		MembershipIdentifier:             a.MembershipIdentifier,
		Name:                             a.Name,
		CreateTime:                       a.CreateTime,
		UpdateTime:                       a.UpdateTime,
		ID:                               a.IDNamespaceAssociationIdentifier,
		MembershipID:                     a.MembershipIdentifier,
		CollaborationID:                  a.CollaborationIdentifier,
	}
}

func (b *InMemoryBackend) CreateIDNamespaceAssociation(
	membershipID, name, description string,
	inputReferenceConfig map[string]any,
	idMappingConfig map[string]any,
	tags map[string]string,
) (*IDNamespaceAssociation, error) {
	b.mu.Lock("CreateIDNamespaceAssociation")
	defer b.mu.Unlock()
	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	collab, _ := b.collaborations.Get(mem.CollaborationIdentifier)
	var collabArn string
	if collab != nil {
		collabArn = collab.Arn
	}
	assoc := &IDNamespaceAssociation{
		IDNamespaceAssociationIdentifier: id,
		Arn:                              b.idNamespaceAssocARN(membershipID, id),
		CollaborationArn:                 collabArn,
		CollaborationIdentifier:          mem.CollaborationIdentifier,
		MembershipArn:                    mem.Arn,
		MembershipIdentifier:             membershipID,
		Name:                             name,
		Description:                      description,
		InputReferenceConfig:             inputReferenceConfig,
		IDMappingConfig:                  idMappingConfig,
		CreateTime:                       ts,
		UpdateTime:                       ts,
		Tags:                             tags,
		ID:                               id,
		MembershipID:                     membershipID,
		CollaborationID:                  mem.CollaborationIdentifier,
	}
	b.idNamespaceAssociations.Put(assoc)
	if len(tags) > 0 {
		b.tagsByArn[assoc.Arn] = maps.Clone(tags)
	}

	return assoc, nil
}

func (b *InMemoryBackend) GetIDNamespaceAssociation(
	membershipID, assocID string,
) (*IDNamespaceAssociation, error) {
	b.mu.RLock("GetIDNamespaceAssociation")
	defer b.mu.RUnlock()
	assoc, ok := b.idNamespaceAssociations.Get(membershipKey(membershipID, assocID))
	if !ok {
		return nil, ErrNotFound
	}

	return assoc, nil
}

func (b *InMemoryBackend) ListIDNamespaceAssociations(
	membershipID, maxResults, nextToken string,
) ([]*IDNamespaceAssociationSummary, string, error) {
	b.mu.RLock("ListIDNamespaceAssociations")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listItems(
		b.idNamespaceAssociationsByMembership.Get(membershipID),
		nil,
		toIDNamespaceAssociationSummary,
		func(a, c *IDNamespaceAssociationSummary) bool {
			return a.IDNamespaceAssociationIdentifier < c.IDNamespaceAssociationIdentifier
		},
		maxResults, nextToken,
	)

	return page, next, nil
}

func (b *InMemoryBackend) UpdateIDNamespaceAssociation(
	membershipID, assocID, description string,
	idMappingConfig map[string]any,
) (*IDNamespaceAssociation, error) {
	b.mu.Lock("UpdateIDNamespaceAssociation")
	defer b.mu.Unlock()
	assoc, ok := b.idNamespaceAssociations.Get(membershipKey(membershipID, assocID))
	if !ok {
		return nil, ErrNotFound
	}
	if description != "" {
		assoc.Description = description
	}
	if idMappingConfig != nil {
		assoc.IDMappingConfig = idMappingConfig
	}
	assoc.UpdateTime = b.now()

	return assoc, nil
}

func (b *InMemoryBackend) DeleteIDNamespaceAssociation(membershipID, assocID string) error {
	b.mu.Lock("DeleteIDNamespaceAssociation")
	defer b.mu.Unlock()
	key := membershipKey(membershipID, assocID)
	assoc, ok := b.idNamespaceAssociations.Get(key)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, assoc.Arn)
	b.idNamespaceAssociations.Delete(key)

	return nil
}

func (b *InMemoryBackend) GetCollaborationIDNamespaceAssociation(
	collaborationID, assocID string,
) (*IDNamespaceAssociation, error) {
	b.mu.RLock("GetCollaborationIDNamespaceAssociation")
	defer b.mu.RUnlock()
	var found *IDNamespaceAssociation
	b.idNamespaceAssociations.Range(func(a *IDNamespaceAssociation) bool {
		if a.CollaborationIdentifier == collaborationID && a.IDNamespaceAssociationIdentifier == assocID {
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

func (b *InMemoryBackend) ListCollaborationIDNamespaceAssociations(
	collaborationID, maxResults, nextToken string,
) ([]*IDNamespaceAssociationSummary, string, error) {
	b.mu.RLock("ListCollaborationIDNamespaceAssociations")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listNestedItems(
		b.idNamespaceAssociations.All(),
		func(a *IDNamespaceAssociation) bool { return a.CollaborationIdentifier == collaborationID },
		toIDNamespaceAssociationSummary,
		func(a, c *IDNamespaceAssociationSummary) bool {
			return a.IDNamespaceAssociationIdentifier < c.IDNamespaceAssociationIdentifier
		},
		maxResults,
		nextToken,
	)

	return page, next, nil
}
