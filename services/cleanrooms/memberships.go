package cleanrooms

import (
	"maps"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) membershipARN(id string) string {
	return arn.Build("cleanrooms", b.region, b.accountID, "membership/"+id)
}

func (b *InMemoryBackend) CreateMembership(
	collaborationID, queryLogStatus string,
	memberAbilities []string,
	defaultResultConfiguration map[string]any,
	paymentConfiguration map[string]any,
	tags map[string]string,
) (*Membership, error) {
	b.mu.Lock("CreateMembership")
	defer b.mu.Unlock()
	if collaborationID == "" {
		return nil, ErrValidation
	}
	collab, ok := b.collaborations.Get(collaborationID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	m := &Membership{
		MembershipIdentifier:            id,
		Arn:                             b.membershipARN(id),
		CollaborationIdentifier:         collaborationID,
		CollaborationArn:                collab.Arn,
		CollaborationCreatorAccountID:   collab.CreatorAccountID,
		CollaborationCreatorDisplayName: collab.CreatorDisplayName,
		CollaborationName:               collab.Name,
		Status:                          statusActive,
		QueryLogStatus:                  queryLogStatus,
		MemberAbilities:                 memberAbilities,
		DefaultResultConfiguration:      defaultResultConfiguration,
		PaymentConfiguration:            paymentConfiguration,
		CreateTime:                      ts,
		UpdateTime:                      ts,
		ID:                              id,
		CollaborationID:                 collaborationID,
	}
	b.memberships.Put(m)
	if len(tags) > 0 {
		b.tagsByArn[m.Arn] = maps.Clone(tags)
	}

	return m, nil
}

func (b *InMemoryBackend) GetMembership(id string) (*Membership, error) {
	b.mu.RLock("GetMembership")
	defer b.mu.RUnlock()
	m, ok := b.memberships.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return m, nil
}

func (b *InMemoryBackend) ListMemberships(
	status, maxResults, nextToken string,
) ([]*MembershipSummary, string) {
	b.mu.RLock("ListMemberships")
	defer b.mu.RUnlock()
	var items []*MembershipSummary
	for _, m := range b.memberships.All() {
		if status != "" && m.Status != status {
			continue
		}
		items = append(items, &MembershipSummary{
			MembershipIdentifier:            m.MembershipIdentifier,
			Arn:                             m.Arn,
			CollaborationIdentifier:         m.CollaborationIdentifier,
			CollaborationArn:                m.CollaborationArn,
			CollaborationCreatorAccountID:   m.CollaborationCreatorAccountID,
			CollaborationCreatorDisplayName: m.CollaborationCreatorDisplayName,
			CollaborationName:               m.CollaborationName,
			Status:                          m.Status,
			MemberAbilities:                 m.MemberAbilities,
			CreateTime:                      m.CreateTime,
			UpdateTime:                      m.UpdateTime,
			ID:                              m.MembershipIdentifier,
			CollaborationID:                 m.CollaborationIdentifier,
		})
	}
	sort.Slice(
		items,
		func(i, j int) bool { return items[i].MembershipIdentifier < items[j].MembershipIdentifier },
	)
	page, next := paginate(items, maxResults, nextToken)

	return page, next
}

func (b *InMemoryBackend) UpdateMembership(
	id, queryLogStatus string,
	defaultResultConfiguration map[string]any,
) (*Membership, error) {
	b.mu.Lock("UpdateMembership")
	defer b.mu.Unlock()
	m, ok := b.memberships.Get(id)
	if !ok {
		return nil, ErrNotFound
	}
	if queryLogStatus != "" {
		m.QueryLogStatus = queryLogStatus
	}
	if defaultResultConfiguration != nil {
		m.DefaultResultConfiguration = defaultResultConfiguration
	}
	m.UpdateTime = b.now()

	return m, nil
}

func (b *InMemoryBackend) DeleteMembership(id string) error {
	b.mu.Lock("DeleteMembership")
	defer b.mu.Unlock()
	m, ok := b.memberships.Get(id)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, m.Arn)
	b.memberships.Delete(id)

	return nil
}
