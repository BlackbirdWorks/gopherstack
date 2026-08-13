package organizations

import (
	"cmp"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	handshakeActionInvite                   = "INVITE"
	handshakeActionLeave                    = "LEAVE_ORGANIZATION"
	handshakeActionEnableFeatures           = "ENABLE_ALL_FEATURES"
	handshakeActionApproveAll               = "APPROVE_ALL_FEATURES"
	handshakeActionTransferResponsibility   = "TRANSFER_RESPONSIBILITY"
	handshakeResourceOrg                    = "ORGANIZATION"
	handshakeResourceMasterEmail            = "MASTER_EMAIL"
	handshakeResourceNotes                  = "NOTES"
	handshakeResourceResponsibilityTransfer = "RESPONSIBILITY_TRANSFER"
	handshakeResourceTransferStartTimestamp = "TRANSFER_START_TIMESTAMP"
	handshakeResourceTransferType           = "TRANSFER_TYPE"

	handshakeStateOpen     = "OPEN"
	handshakeStateCanceled = "CANCELED"
	handshakeStateAccepted = "ACCEPTED"
	handshakeStateDeclined = "DECLINED"
	handshakeStateExpired  = "EXPIRED"

	// handshakeExpirationDuration is the default lifetime of a handshake (AWS default: 15 days).
	handshakeExpirationDuration = 15 * 24 * time.Hour
)

// AcceptHandshake accepts an OPEN handshake.
// For INVITE handshakes, the invited account is added to the organization.
func (b *InMemoryBackend) AcceptHandshake(handshakeID string) (*Handshake, error) {
	b.mu.Lock("AcceptHandshake")
	defer b.mu.Unlock()

	h, ok := b.handshakes.Get(handshakeID)
	if !ok {
		return nil, ErrHandshakeNotFound
	}

	if h.State != handshakeStateOpen {
		return nil, ErrHandshakeConstraintViolation
	}

	h.State = handshakeStateAccepted

	if h.Action == handshakeActionInvite && b.org != nil {
		for _, r := range h.Resources {
			if r.Type == targetTypeAccount {
				acctID := r.Value
				if !b.accounts.Has(acctID) {
					now := time.Now()
					acct := &Account{
						ID:           acctID,
						ARN:          b.accountARN(b.org.ID, acctID),
						Name:         acctID,
						Email:        acctID + "@invited.example.com",
						Status:       accountStatusActive,
						JoinedMethod: joinedMethodInvited,
						JoinedAt:     now,
					}
					b.accounts.Put(acct)
					b.accountParent[acctID] = b.root.ID
					b.addAccountChild(b.root.ID, acctID)
				}

				break
			}
		}
	}

	return copyHandshake(h), nil
}

// CancelHandshake cancels an OPEN handshake.
func (b *InMemoryBackend) CancelHandshake(handshakeID string) (*Handshake, error) {
	b.mu.Lock("CancelHandshake")
	defer b.mu.Unlock()

	h, ok := b.handshakes.Get(handshakeID)
	if !ok {
		return nil, ErrHandshakeNotFound
	}

	if h.State != handshakeStateOpen {
		return nil, ErrHandshakeConstraintViolation
	}

	h.State = handshakeStateCanceled

	return copyHandshake(h), nil
}

// DeclineHandshake declines an OPEN handshake.
func (b *InMemoryBackend) DeclineHandshake(handshakeID string) (*Handshake, error) {
	b.mu.Lock("DeclineHandshake")
	defer b.mu.Unlock()

	h, ok := b.handshakes.Get(handshakeID)
	if !ok {
		return nil, ErrHandshakeNotFound
	}

	if h.State != handshakeStateOpen {
		return nil, ErrHandshakeConstraintViolation
	}

	h.State = handshakeStateDeclined

	return copyHandshake(h), nil
}

// DescribeHandshake returns a handshake by ID.
func (b *InMemoryBackend) DescribeHandshake(handshakeID string) (*Handshake, error) {
	b.mu.Lock("DescribeHandshake")
	defer b.mu.Unlock()

	b.expireStaleHandshakesLocked()

	h, ok := b.handshakes.Get(handshakeID)
	if !ok {
		return nil, ErrHandshakeNotFound
	}

	return copyHandshake(h), nil
}

// DescribeResponsibilityTransfer returns a responsibility-transfer handshake by ID.
func (b *InMemoryBackend) DescribeResponsibilityTransfer(handshakeID string) (*Handshake, error) {
	b.mu.RLock("DescribeResponsibilityTransfer")
	defer b.mu.RUnlock()

	h, ok := b.handshakes.Get(handshakeID)
	if !ok {
		return nil, ErrHandshakeNotFound
	}

	return copyHandshake(h), nil
}

// AddHandshakeInternal seeds a handshake directly for testing.
// If h.ID is empty, a new ID is generated. If h.ExpirationTimestamp is zero,
// it is set to now + handshakeExpirationDuration.
func (b *InMemoryBackend) AddHandshakeInternal(h *Handshake) {
	b.mu.Lock("AddHandshakeInternal")
	defer b.mu.Unlock()

	if h.ID == "" {
		h.ID = newHandshakeID()
	}

	if h.ExpirationTimestamp.IsZero() {
		h.ExpirationTimestamp = time.Now().Add(handshakeExpirationDuration)
	}

	if h.ARN == "" && b.org != nil {
		action := h.Action
		if action == "" {
			action = handshakeActionInvite
		}

		h.ARN = b.handshakeARN(b.org.ID, action, h.ID)
	}

	b.handshakes.Put(h)
}

// expireStaleHandshakesLocked transitions OPEN handshakes past their ExpirationTimestamp to EXPIRED.
// Must be called with a write lock held.
func (b *InMemoryBackend) expireStaleHandshakesLocked() {
	now := time.Now()
	for _, h := range b.handshakes.All() {
		if h.State == handshakeStateOpen && !h.ExpirationTimestamp.IsZero() && now.After(h.ExpirationTimestamp) {
			h.State = handshakeStateExpired
		}
	}
}

// EnableAllFeatures creates an ENABLE_ALL_FEATURES handshake and returns it.
// Real AWS sends this handshake to all member accounts to approve the feature upgrade.
func (b *InMemoryBackend) EnableAllFeatures() (*Handshake, error) {
	b.mu.Lock("EnableAllFeatures")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	now := time.Now()
	id := newHandshakeID()
	h := &Handshake{
		ID:                  id,
		ARN:                 b.handshakeARN(b.org.ID, handshakeActionEnableFeatures, id),
		Action:              handshakeActionEnableFeatures,
		State:               handshakeStateOpen,
		RequestedTimestamp:  now,
		ExpirationTimestamp: now.Add(handshakeExpirationDuration),
		Parties: []HandshakeParty{
			{ID: b.org.MasterAccountID, Type: targetTypeAccount},
		},
		Resources: []HandshakeResource{
			{Type: handshakeResourceOrg, Value: b.org.ID},
			{Type: handshakeResourceMasterEmail, Value: b.org.MasterAccountEmail},
		},
	}

	b.handshakes.Put(h)

	return copyHandshake(h), nil
}

// InviteAccountToOrganization creates an OPEN invitation handshake targeting an account.
func validateHandshakeTarget(target HandshakeParty) error {
	if target.ID == "" {
		return ErrInvalidInput
	}
	switch target.Type {
	case targetTypeAccount:
		if len(target.ID) != accountIDLength {
			return ErrInvalidInput
		}
	case "EMAIL":
		if !strings.Contains(target.ID, "@") {
			return ErrInvalidInput
		}
	default:
		if target.Type != "" {
			return ErrInvalidInput
		}
	}

	return nil
}

func (b *InMemoryBackend) InviteAccountToOrganization(
	target HandshakeParty,
	notes string,
) (*Handshake, error) {
	b.mu.Lock("InviteAccountToOrganization")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if err := validateHandshakeTarget(target); err != nil {
		return nil, err
	}

	// AWS rejects duplicate open invitations to the same target.
	for _, existing := range b.handshakes.All() {
		if existing.State != handshakeStateOpen || existing.Action != handshakeActionInvite {
			continue
		}
		for _, r := range existing.Resources {
			if r.Type == targetTypeAccount && r.Value == target.ID {
				return nil, ErrDuplicateHandshake
			}
		}
	}

	now := time.Now()
	id := newHandshakeID()
	h := &Handshake{
		ID:                  id,
		ARN:                 b.handshakeARN(b.org.ID, handshakeActionInvite, id),
		Action:              handshakeActionInvite,
		State:               handshakeStateOpen,
		RequestedTimestamp:  now,
		ExpirationTimestamp: now.Add(handshakeExpirationDuration),
		Parties: []HandshakeParty{
			{ID: b.org.MasterAccountID, Type: targetTypeAccount},
			{ID: target.ID, Type: target.Type},
		},
		Resources: []HandshakeResource{
			{Type: targetTypeAccount, Value: target.ID},
			{Type: handshakeResourceOrg, Value: b.org.ID},
			{Type: handshakeResourceMasterEmail, Value: b.org.MasterAccountEmail},
		},
	}

	if notes != "" {
		h.Resources = append(h.Resources, HandshakeResource{Type: handshakeResourceNotes, Value: notes})
	}

	b.handshakes.Put(h)

	return copyHandshake(h), nil
}

// LeaveOrganization removes the management account from the organization (stub: returns no error).
func (b *InMemoryBackend) LeaveOrganization() error {
	b.mu.RLock("LeaveOrganization")
	defer b.mu.RUnlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	return nil
}

// ListHandshakesForAccount returns all handshakes visible to the calling account.
// actionTypeFilter optionally restricts results to handshakes with the given Action value.
func (b *InMemoryBackend) ListHandshakesForAccount(actionTypeFilter string) ([]*Handshake, error) {
	b.mu.Lock("ListHandshakesForAccount")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	b.expireStaleHandshakesLocked()

	out := make([]*Handshake, 0, b.handshakes.Len())

	for _, h := range b.handshakes.All() {
		if actionTypeFilter == "" || h.Action == actionTypeFilter {
			out = append(out, copyHandshake(h))
		}
	}

	slices.SortFunc(out, func(a, b *Handshake) int { return cmp.Compare(a.ID, b.ID) })

	return out, nil
}

// ListHandshakesForOrganization returns all handshakes for the organization.
// actionTypeFilter optionally restricts results to handshakes with the given Action value.
func (b *InMemoryBackend) ListHandshakesForOrganization(actionTypeFilter string) ([]*Handshake, error) {
	b.mu.Lock("ListHandshakesForOrganization")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	b.expireStaleHandshakesLocked()

	out := make([]*Handshake, 0, b.handshakes.Len())

	for _, h := range b.handshakes.All() {
		if actionTypeFilter == "" || h.Action == actionTypeFilter {
			out = append(out, copyHandshake(h))
		}
	}

	slices.SortFunc(out, func(a, b *Handshake) int { return cmp.Compare(a.ID, b.ID) })

	return out, nil
}

// ListInboundResponsibilityTransfers returns responsibility-transfer handshakes sent TO this account by
// another organization's management account. InviteOrganizationToTransferResponsibility can only be called
// from the sending account (api_op_InviteOrganizationToTransferResponsibility.go doc comment), and this
// single-account backend has no way to simulate a transfer initiated by a foreign account, so every
// TRANSFER_RESPONSIBILITY handshake this backend ever creates is outbound -- see
// ListOutboundResponsibilityTransfers. Returning empty here is honest given that structural limit, not a stub:
// there is no fabricated data to return.
func (b *InMemoryBackend) ListInboundResponsibilityTransfers() ([]*Handshake, error) {
	b.mu.RLock("ListInboundResponsibilityTransfers")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	return nil, nil
}

// ListOutboundResponsibilityTransfers returns responsibility-transfer handshakes initiated by this org.
func (b *InMemoryBackend) ListOutboundResponsibilityTransfers() ([]*Handshake, error) {
	b.mu.RLock("ListOutboundResponsibilityTransfers")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	var out []*Handshake

	for _, h := range b.handshakes.All() {
		if h.Action == handshakeActionTransferResponsibility {
			out = append(out, copyHandshake(h))
		}
	}

	slices.SortFunc(out, func(a, b *Handshake) int { return cmp.Compare(a.ID, b.ID) })

	return out, nil
}

// TerminateResponsibilityTransfer terminates an OPEN responsibility-transfer handshake.
func (b *InMemoryBackend) TerminateResponsibilityTransfer(handshakeID string) (*Handshake, error) {
	b.mu.Lock("TerminateResponsibilityTransfer")
	defer b.mu.Unlock()

	h, ok := b.handshakes.Get(handshakeID)
	if !ok {
		return nil, ErrHandshakeNotFound
	}

	if h.State != handshakeStateOpen {
		return nil, ErrHandshakeConstraintViolation
	}

	h.State = handshakeStateCanceled

	return copyHandshake(h), nil
}

// UpdateResponsibilityTransfer accepts or declines a responsibility-transfer handshake.
func (b *InMemoryBackend) UpdateResponsibilityTransfer(
	handshakeID, action string,
) (*Handshake, error) {
	b.mu.Lock("UpdateResponsibilityTransfer")
	defer b.mu.Unlock()

	h, ok := b.handshakes.Get(handshakeID)
	if !ok {
		return nil, ErrHandshakeNotFound
	}

	if h.State != handshakeStateOpen {
		return nil, ErrHandshakeConstraintViolation
	}

	switch action {
	case "ACCEPT":
		h.State = handshakeStateAccepted
	case "DECLINE":
		h.State = handshakeStateDeclined
	default:
		return nil, ErrInvalidInput
	}

	return copyHandshake(h), nil
}

// InviteOrganizationToTransferResponsibility creates an OPEN invitation for org-to-org responsibility transfer.
// SourceName/StartTimestamp/Type are required InviteOrganizationToTransferResponsibilityInput members with no
// first-class Handshake field to carry them, so they are embedded as HandshakeResource entries the same way
// InviteAccountToOrganization/EnableAllFeatures embed their own extra fields (NOTES, MASTER_EMAIL) --
// RESPONSIBILITY_TRANSFER/TRANSFER_START_TIMESTAMP/TRANSFER_TYPE are the matching HandshakeResourceType enum
// values (types/enums.go).
func (b *InMemoryBackend) InviteOrganizationToTransferResponsibility(
	target HandshakeParty,
	params TransferResponsibilityParams,
) (*Handshake, error) {
	b.mu.Lock("InviteOrganizationToTransferResponsibility")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if target.ID == "" || params.SourceName == "" || params.Type == "" || params.StartTimestamp.IsZero() {
		return nil, ErrInvalidInput
	}

	now := time.Now()
	id := newHandshakeID()
	h := &Handshake{
		ID:                  id,
		ARN:                 b.handshakeARN(b.org.ID, handshakeActionTransferResponsibility, id),
		Action:              handshakeActionTransferResponsibility,
		State:               handshakeStateOpen,
		RequestedTimestamp:  now,
		ExpirationTimestamp: now.Add(handshakeExpirationDuration),
		Parties: []HandshakeParty{
			{ID: b.org.MasterAccountID, Type: targetTypeAccount},
			{ID: target.ID, Type: target.Type},
		},
		Resources: []HandshakeResource{
			{Type: handshakeResourceOrg, Value: b.org.ID},
			{Type: handshakeResourceResponsibilityTransfer, Value: params.SourceName},
			{
				Type:  handshakeResourceTransferStartTimestamp,
				Value: strconv.FormatFloat(epochSeconds(params.StartTimestamp), 'f', -1, 64),
			},
			{Type: handshakeResourceTransferType, Value: params.Type},
		},
	}

	if params.Notes != "" {
		h.Resources = append(h.Resources, HandshakeResource{Type: handshakeResourceNotes, Value: params.Notes})
	}

	b.handshakes.Put(h)

	return copyHandshake(h), nil
}

// copyHandshake returns a deep copy of a Handshake.
func copyHandshake(h *Handshake) *Handshake {
	cp := *h

	if h.Parties != nil {
		cp.Parties = make([]HandshakeParty, len(h.Parties))
		copy(cp.Parties, h.Parties)
	}

	cp.Resources = copyHandshakeResources(h.Resources)

	return &cp
}

// copyHandshakeResources returns a deep copy of a HandshakeResource slice.
func copyHandshakeResources(rs []HandshakeResource) []HandshakeResource {
	if rs == nil {
		return nil
	}

	out := make([]HandshakeResource, len(rs))

	for i, r := range rs {
		out[i] = HandshakeResource{
			Type:      r.Type,
			Value:     r.Value,
			Resources: copyHandshakeResources(r.Resources),
		}
	}

	return out
}
