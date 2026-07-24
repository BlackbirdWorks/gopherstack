package cleanrooms

import (
	"maps"
	"slices"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
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
		AccountID:     b.accountID,
		DisplayName:   creatorDisplayName,
		Abilities:     creatorMemberAbilities,
		Status:        statusActive,
		PaymentConfig: defaultPaymentConfig(creatorMemberAbilities, nil),
		CreateTime:    ts,
		UpdateTime:    ts,
	})
	for _, m := range members {
		memberSummaries = append(memberSummaries, &MemberSummary{
			AccountID:     m.AccountID,
			DisplayName:   m.DisplayName,
			Abilities:     m.Abilities,
			Status:        "INVITED",
			PaymentConfig: defaultPaymentConfig(m.Abilities, m.PaymentConfig),
			CreateTime:    ts,
			UpdateTime:    ts,
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

	// Real AWS auto-creates a membership for the collaboration creator (the
	// Collaboration response's membershipArn/membershipId document "your
	// membership within the collaboration"). Mirror that here so
	// GetCollaboration/ListCollaborations reflect a real membershipArn/Id,
	// and so DeleteCollaboration has a real membership to transition to
	// COLLABORATION_DELETED.
	creatorMembership := b.createMembershipLocked(
		collab, queryLogStatus, creatorMemberAbilities, nil, memberSummaries[0].PaymentConfig, nil,
	)
	collab.MembershipArn = creatorMembership.Arn
	collab.MembershipID = creatorMembership.ID
	memberSummaries[0].MembershipArn = creatorMembership.Arn
	memberSummaries[0].MembershipID = creatorMembership.ID

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
			ID:                      c.ID,
			Arn:                     c.Arn,
			Name:                    c.Name,
			CreatorAccountID:        c.CreatorAccountID,
			CreatorDisplayName:      c.CreatorDisplayName,
			MemberStatus:            statusActive,
			MembershipArn:           c.MembershipArn,
			MembershipID:            c.MembershipID,
			CreateTime:              c.CreateTime,
			UpdateTime:              c.UpdateTime,
		})
	}
	sort.Slice(
		items,
		func(i, j int) bool { return items[i].ID < items[j].ID },
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

	// Real AWS transitions every ACTIVE membership under a deleted
	// collaboration to COLLABORATION_DELETED (MembershipStatus documents
	// this exact enum value) rather than deleting the membership rows --
	// GetMembership/ListMemberships must keep working after the
	// collaboration is gone, for audit/history purposes.
	for _, m := range b.memberships.All() {
		if m.CollaborationID == id && m.Status == statusActive {
			m.Status = "COLLABORATION_DELETED"
			m.UpdateTime = b.now()
		}
	}

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
	// Real AWS: "The removed member is placed in the Removed status and
	// can't interact with the collaboration" -- the member stays in the
	// list (ListMembers/GetCollaboration must still show them), it is not
	// spliced out.
	for _, m := range c.Members {
		if m.AccountID == accountID {
			m.Status = "REMOVED"
			m.UpdateTime = b.now()

			return nil
		}
	}

	return ErrNotFound
}

func (b *InMemoryBackend) CreateCollaborationChangeRequest(
	collaborationID string,
	changes []map[string]any,
) (*CollaborationChangeRequest, error) {
	b.mu.Lock("CreateCollaborationChangeRequest")
	defer b.mu.Unlock()
	if len(changes) == 0 {
		return nil, ErrValidation
	}
	collab, ok := b.collaborations.Get(collaborationID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	req := &CollaborationChangeRequest{
		ChangeRequestIdentifier: id,
		ID:                      id,
		CollaborationIdentifier: collaborationID,
		CollaborationID:         collaborationID,
		CollaborationArn:        collab.Arn,
		Status:                  "PENDING",
		Changes:                 changes,
		// This backend does not model per-collaboration auto-approval
		// settings (autoApprovedChangeTypes on Collaboration is deferred,
		// see PARITY.md), so change requests always require an explicit
		// UpdateCollaborationChangeRequest action.
		IsAutoApproved: false,
		CreateTime:     ts,
		UpdateTime:     ts,
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
		func(i, j int) bool { return items[i].ID < items[j].ID },
	)
	page, next := paginate(items, maxResults, nextToken)

	return page, next, nil
}

// changeRequestNextStatus maps an UpdateCollaborationChangeRequest `action`
// (APPROVE/DENY/CANCEL/COMMIT -- ChangeRequestAction) plus the change
// request's current status to its next status, matching AWS's documented
// change-request lifecycle: a PENDING request may be approved, denied, or
// cancelled; an APPROVED request may be committed or cancelled. Any other
// (action, status) pair is an invalid transition.
func changeRequestNextStatus(action, current string) (string, bool) {
	transitions := map[string]map[string]string{
		"PENDING":  {"APPROVE": "APPROVED", "DENY": "DENIED", changeRequestActionCancel: statusCancelled},
		"APPROVED": {"COMMIT": "COMMITTED", changeRequestActionCancel: statusCancelled},
	}
	next, ok := transitions[current][action]

	return next, ok
}

func (b *InMemoryBackend) UpdateCollaborationChangeRequest(
	collaborationID, changeRequestID, action string,
) (*CollaborationChangeRequest, error) {
	b.mu.Lock("UpdateCollaborationChangeRequest")
	defer b.mu.Unlock()
	switch action {
	case "APPROVE", "DENY", changeRequestActionCancel, "COMMIT":
	default:
		return nil, ErrValidation
	}
	req, ok := b.changeRequests.Get(collaborationKey(collaborationID, changeRequestID))
	if !ok {
		return nil, ErrNotFound
	}
	next, ok := changeRequestNextStatus(action, req.Status)
	if !ok {
		return nil, ErrConflict
	}
	req.Status = next
	req.UpdateTime = b.now()

	return req, nil
}
