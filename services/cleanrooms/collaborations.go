package cleanrooms

import (
	"maps"
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/google/uuid"
)

func (b *InMemoryBackend) collaborationARN(id string) string {
	return arn.Build("cleanrooms", b.region, b.accountID, "collaboration/"+id)
}

func (b *InMemoryBackend) CreateCollaboration(
	name, description, creatorDisplayName string,
	creatorMemberAbilities []string,
	members []MemberSpec,
	queryLogStatus string,
	tags map[string]string,
) (*Collaboration, error) {
	b.mu.Lock("CreateCollaboration")
	defer b.mu.Unlock()
	if name == "" {
		return nil, ErrValidation
	}
	id := uuid.NewString()
	ts := b.now()
	memberSummaries := make([]*MemberSummary, 0, len(members)+1)
	memberSummaries = append(memberSummaries, &MemberSummary{
		AccountID:   b.accountID,
		DisplayName: creatorDisplayName,
		Abilities:   creatorMemberAbilities,
		Status:      statusActive,
		CreateTime:  ts,
		UpdateTime:  ts,
	})
	for _, m := range members {
		memberSummaries = append(memberSummaries, &MemberSummary{
			AccountID:   m.AccountID,
			DisplayName: m.DisplayName,
			Abilities:   m.Abilities,
			Status:      "INVITED",
			CreateTime:  ts,
			UpdateTime:  ts,
		})
	}
	collab := &Collaboration{
		CollaborationIdentifier: id,
		ID:                      id,
		Arn:                     b.collaborationARN(id),
		Name:                    name,
		Description:             description,
		CreatorAccountID:        b.accountID,
		CreatorDisplayName:      creatorDisplayName,
		MemberStatus:            statusActive,
		MemberAbilities:         creatorMemberAbilities,
		Members:                 memberSummaries,
		QueryLogStatus:          queryLogStatus,
		CreateTime:              ts,
		UpdateTime:              ts,
		Tags:                    tags,
	}
	b.collaborations.Put(collab)
	if len(tags) > 0 {
		b.tagsByArn[collab.Arn] = maps.Clone(tags)
	}

	return collab, nil
}

func (b *InMemoryBackend) GetCollaboration(id string) (*Collaboration, error) {
	b.mu.RLock("GetCollaboration")
	defer b.mu.RUnlock()
	c, ok := b.collaborations.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return c, nil
}

func (b *InMemoryBackend) ListCollaborations(
	_, maxResults, nextToken string,
) ([]*CollaborationSummary, string) {
	b.mu.RLock("ListCollaborations")
	defer b.mu.RUnlock()
	all := b.collaborations.All()
	items := make([]*CollaborationSummary, 0, len(all))
	for _, c := range all {
		items = append(items, &CollaborationSummary{
			CollaborationIdentifier: c.CollaborationIdentifier,
			ID:                      c.CollaborationIdentifier,
			Arn:                     c.Arn,
			Name:                    c.Name,
			CreatorAccountID:        c.CreatorAccountID,
			CreatorDisplayName:      c.CreatorDisplayName,
			MemberStatus:            statusActive,
			CreateTime:              c.CreateTime,
			UpdateTime:              c.UpdateTime,
		})
	}
	sort.Slice(
		items,
		func(i, j int) bool { return items[i].CollaborationIdentifier < items[j].CollaborationIdentifier },
	)
	page, next := paginate(items, maxResults, nextToken)

	return page, next
}

func (b *InMemoryBackend) UpdateCollaboration(
	id, name, description string,
) (*Collaboration, error) {
	b.mu.Lock("UpdateCollaboration")
	defer b.mu.Unlock()
	c, ok := b.collaborations.Get(id)
	if !ok {
		return nil, ErrNotFound
	}
	if name != "" {
		c.Name = name
	}
	if description != "" {
		c.Description = description
	}
	c.UpdateTime = b.now()

	return c, nil
}

func (b *InMemoryBackend) DeleteCollaboration(id string) error {
	b.mu.Lock("DeleteCollaboration")
	defer b.mu.Unlock()
	c, ok := b.collaborations.Get(id)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, c.Arn)
	b.collaborations.Delete(id)

	return nil
}

func (b *InMemoryBackend) ListMembers(
	collaborationID string,
	maxResults, nextToken string,
) ([]*MemberSummary, string, error) {
	b.mu.RLock("ListMembers")
	defer b.mu.RUnlock()
	c, ok := b.collaborations.Get(collaborationID)
	if !ok {
		return nil, "", ErrNotFound
	}
	members := make([]*MemberSummary, len(c.Members))
	copy(members, c.Members)
	page, next := paginate(members, maxResults, nextToken)

	return page, next, nil
}

func (b *InMemoryBackend) DeleteMember(collaborationID, accountID string) error {
	b.mu.Lock("DeleteMember")
	defer b.mu.Unlock()
	c, ok := b.collaborations.Get(collaborationID)
	if !ok {
		return ErrNotFound
	}
	for i, m := range c.Members {
		if m.AccountID == accountID {
			c.Members = append(c.Members[:i], c.Members[i+1:]...)

			return nil
		}
	}

	return ErrNotFound
}

func (b *InMemoryBackend) CreateCollaborationChangeRequest(
	collaborationID, changeRequestType string,
	details map[string]any,
) (*CollaborationChangeRequest, error) {
	b.mu.Lock("CreateCollaborationChangeRequest")
	defer b.mu.Unlock()
	collab, ok := b.collaborations.Get(collaborationID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	req := &CollaborationChangeRequest{
		ChangeRequestIdentifier: id,
		CollaborationIdentifier: collaborationID,
		CollaborationArn:        collab.Arn,
		Status:                  "PENDING",
		Type:                    changeRequestType,
		Details:                 details,
		CreateTime:              ts,
		UpdateTime:              ts,
	}
	b.changeRequests.Put(req)

	return req, nil
}

func (b *InMemoryBackend) GetCollaborationChangeRequest(
	collaborationID, changeRequestID string,
) (*CollaborationChangeRequest, error) {
	b.mu.RLock("GetCollaborationChangeRequest")
	defer b.mu.RUnlock()
	req, ok := b.changeRequests.Get(collaborationKey(collaborationID, changeRequestID))
	if !ok {
		return nil, ErrNotFound
	}

	return req, nil
}

func (b *InMemoryBackend) ListCollaborationChangeRequests(
	collaborationID, maxResults, nextToken string,
) ([]*CollaborationChangeRequest, string, error) {
	b.mu.RLock("ListCollaborationChangeRequests")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, "", ErrNotFound
	}
	items := slices.Clone(b.changeRequestsByCollaboration.Get(collaborationID))
	sort.Slice(
		items,
		func(i, j int) bool { return items[i].ChangeRequestIdentifier < items[j].ChangeRequestIdentifier },
	)
	page, next := paginate(items, maxResults, nextToken)

	return page, next, nil
}

func (b *InMemoryBackend) UpdateCollaborationChangeRequest(
	collaborationID, changeRequestID, status string,
) (*CollaborationChangeRequest, error) {
	b.mu.Lock("UpdateCollaborationChangeRequest")
	defer b.mu.Unlock()
	req, ok := b.changeRequests.Get(collaborationKey(collaborationID, changeRequestID))
	if !ok {
		return nil, ErrNotFound
	}
	req.Status = status
	req.UpdateTime = b.now()

	return req, nil
}
