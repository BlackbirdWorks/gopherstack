package cleanrooms

import (
	"fmt"
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

// DeleteCollaboration deletes the collaboration identified by id. A
// nonexistent id is a no-op success, not ErrNotFound: cleanrooms@v1.49.4's
// awsRestjson1_deserializeOpErrorDeleteCollaboration switch does not type
// ResourceNotFoundException at all (only AccessDeniedException,
// InternalServerException, ThrottlingException, ValidationException), so a
// real client would see an untyped smithy.GenericAPIError instead of any
// modeled exception. Inference: a delete this op's own SDK cannot report
// not-found for must be idempotent instead -- DeleteCollaborationOutput
// carries no fields at all, so an empty success fabricates nothing.
func (b *InMemoryBackend) DeleteCollaboration(id string) error {
	b.mu.Lock("DeleteCollaboration")
	defer b.mu.Unlock()
	c, ok := b.collaborations.Get(id)
	if !ok {
		return nil
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

// validChangeSpecificationTypes are the real ChangeSpecificationType enum
// values (types.ChangeSpecificationTypeMember/-Collaboration).
func validChangeSpecificationTypes() map[string]bool {
	return map[string]bool{changeSpecTypeMember: true, changeSpecTypeCollaboration: true}
}

// validChangeTypes are the real ChangeType enum values (types/enums.go).
func validChangeTypes() map[string]bool {
	return map[string]bool{
		"ADD_MEMBER":                          true,
		"GRANT_RECEIVE_RESULTS_ABILITY":       true,
		"REVOKE_RECEIVE_RESULTS_ABILITY":      true,
		"EDIT_AUTO_APPROVED_CHANGE_TYPES":     true,
		"ADD_PAYER_CANDIDATE":                 true,
		"REMOVE_PAYER_CANDIDATE":              true,
		"GRANT_CAN_RECEIVE_MODEL_OUTPUT":      true,
		"GRANT_CAN_RECEIVE_INFERENCE_OUTPUT":  true,
		"REVOKE_CAN_RECEIVE_MODEL_OUTPUT":     true,
		"REVOKE_CAN_RECEIVE_INFERENCE_OUTPUT": true,
	}
}

// validateChange checks a single Change against the typed union's real
// required-field/enum constraints (ChangeInput/Change/ChangeSpecification/
// MemberChangeSpecification/CollaborationChangeSpecification -- all "This
// member is required" fields verified against the SDK doc comments).
//
// types.ChangeInput (the real request shape) has no "types" member at all --
// only the response type types.Change does, as a server-computed field (see
// deriveChangeTypes) -- so a real client never sends it and this must not
// require it.
func validateChange(c Change) error {
	if !validChangeSpecificationTypes()[c.SpecificationType] {
		return fmt.Errorf("%w: invalid specificationType %q", ErrValidation, c.SpecificationType)
	}

	for _, t := range c.Types {
		if !validChangeTypes()[t] {
			return fmt.Errorf("%w: invalid change type %q", ErrValidation, t)
		}
	}

	switch c.SpecificationType {
	case changeSpecTypeMember:
		if c.Specification.Member == nil || c.Specification.Member.AccountID == "" {
			return fmt.Errorf("%w: specification.member.accountId is required", ErrValidation)
		}
	case changeSpecTypeCollaboration:
		if c.Specification.Collaboration == nil {
			return fmt.Errorf("%w: specification.collaboration is required", ErrValidation)
		}
	}

	return nil
}

// deriveChangeTypes computes the required-on-response types.Change.Types
// field from a change's specification, since a real client never supplies it
// (see validateChange). This backend doesn't track prior member ability
// state to distinguish grant-vs-revoke, so a MEMBER change is always
// reported as an add, optionally paired with the results-ability grant its
// memberAbilities imply.
func deriveChangeTypes(c Change) []string {
	switch c.SpecificationType {
	case changeSpecTypeMember:
		types := []string{"ADD_MEMBER"}
		for _, a := range c.Specification.Member.MemberAbilities {
			if a == "CAN_RECEIVE_RESULTS" {
				types = append(types, "GRANT_RECEIVE_RESULTS_ABILITY")
			}
		}

		return types
	case changeSpecTypeCollaboration:
		return []string{"EDIT_AUTO_APPROVED_CHANGE_TYPES"}
	default:
		return nil
	}
}

func (b *InMemoryBackend) CreateCollaborationChangeRequest(
	collaborationID string,
	changes []Change,
) (*CollaborationChangeRequest, error) {
	b.mu.Lock("CreateCollaborationChangeRequest")
	defer b.mu.Unlock()
	if len(changes) == 0 {
		return nil, ErrValidation
	}

	for i, c := range changes {
		if err := validateChange(c); err != nil {
			return nil, err
		}
		if len(c.Types) == 0 {
			changes[i].Types = deriveChangeTypes(c)
		}
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

// applyMemberChangeLocked applies a single MEMBER-specification change to
// collab. ADD_MEMBER appends a new invited MemberSummary, matching the
// non-creator member shape CreateCollaboration already produces (status
// INVITED, no membershipArn/Id -- this backend only ever materializes a real
// Membership object for its own account's membership, never for other
// simulated members, see CreateCollaboration). GRANT/REVOKE_RECEIVE_RESULTS_
// ABILITY toggle CAN_RECEIVE_RESULTS on the matching member's abilities, and
// on its Membership.MemberAbilities too when that member has one (i.e. is the
// collaboration's own account). Other MEMBER change types (payer-candidate,
// ML output abilities) touch fields this backend does not model (see
// PARITY.md) and are left as a documented no-op rather than fabricated.
func (b *InMemoryBackend) applyMemberChangeLocked(collab *Collaboration, c Change) {
	spec := c.Specification.Member
	if spec == nil {
		return
	}

	if contains(c.Types, "ADD_MEMBER") {
		b.applyAddMemberLocked(collab, spec)

		return
	}

	grant := contains(c.Types, "GRANT_RECEIVE_RESULTS_ABILITY")
	revoke := contains(c.Types, "REVOKE_RECEIVE_RESULTS_ABILITY")
	if grant || revoke {
		b.applyReceiveResultsAbilityChangeLocked(collab, spec.AccountID, grant)
	}
}

// applyAddMemberLocked appends a new invited MemberSummary for spec, matching
// the non-creator member shape CreateCollaboration already produces (status
// INVITED, no membershipArn/Id -- this backend only ever materializes a real
// Membership object for its own account's membership, never for other
// simulated members, see CreateCollaboration). A no-op if the account is
// already a member.
func (b *InMemoryBackend) applyAddMemberLocked(collab *Collaboration, spec *MemberChangeSpecification) {
	for _, m := range collab.Members {
		if m.AccountID == spec.AccountID {
			return
		}
	}

	ts := b.now()
	collab.Members = append(collab.Members, &MemberSummary{
		AccountID:     spec.AccountID,
		DisplayName:   spec.DisplayName,
		Abilities:     spec.MemberAbilities,
		Status:        "INVITED",
		PaymentConfig: defaultPaymentConfig(spec.MemberAbilities, nil),
		CreateTime:    ts,
		UpdateTime:    ts,
	})
}

// applyReceiveResultsAbilityChangeLocked toggles CAN_RECEIVE_RESULTS on the
// member matching accountID, and on its Membership.MemberAbilities too when
// that member has one (i.e. is the collaboration's own account).
func (b *InMemoryBackend) applyReceiveResultsAbilityChangeLocked(collab *Collaboration, accountID string, grant bool) {
	for _, m := range collab.Members {
		if m.AccountID != accountID {
			continue
		}

		if grant && !contains(m.Abilities, "CAN_RECEIVE_RESULTS") {
			m.Abilities = append(m.Abilities, "CAN_RECEIVE_RESULTS")
		} else if !grant {
			m.Abilities = slices.DeleteFunc(m.Abilities, func(a string) bool { return a == "CAN_RECEIVE_RESULTS" })
		}

		ts := b.now()
		m.UpdateTime = ts

		if m.MembershipID != "" {
			if mem, found := b.memberships.Get(m.MembershipID); found {
				mem.MemberAbilities = m.Abilities
				mem.UpdateTime = ts
			}
		}

		return
	}
}

// applyChangeRequestEffectsLocked applies each committed Change's real
// semantic effect to the parent collaboration. Caller must hold b.mu (write).
func (b *InMemoryBackend) applyChangeRequestEffectsLocked(collab *Collaboration, req *CollaborationChangeRequest) {
	for _, c := range req.Changes {
		switch c.SpecificationType {
		case changeSpecTypeMember:
			b.applyMemberChangeLocked(collab, c)
		case changeSpecTypeCollaboration:
			if c.Specification.Collaboration != nil && contains(c.Types, "EDIT_AUTO_APPROVED_CHANGE_TYPES") {
				collab.AutoApprovedChangeTypes = c.Specification.Collaboration.AutoApprovedChangeTypes
				collab.UpdateTime = b.now()
			}
		}
	}
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

	if next == "COMMITTED" {
		if collab, found := b.collaborations.Get(collaborationID); found {
			b.applyChangeRequestEffectsLocked(collab, req)
		}
	}

	return req, nil
}
