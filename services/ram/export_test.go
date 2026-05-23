package ram

import "time"

// BuiltInPermissionCount is the number of AWS-managed permissions seeded at construction.
//
//nolint:gochecknoglobals // test-only helper that exposes the internal count
var BuiltInPermissionCount = len(awsBuiltInPermissions)

// ResourceShareCount returns the number of resource shares in the backend (including deleted).
func ResourceShareCount(b *InMemoryBackend) int {
	b.mu.RLock("ResourceShareCount")
	defer b.mu.RUnlock()

	return len(b.resourceShares)
}

// PermissionCount returns the number of permissions in the backend (including soft-deleted).
func PermissionCount(b *InMemoryBackend) int {
	b.mu.RLock("PermissionCount")
	defer b.mu.RUnlock()

	return len(b.permissions)
}

// InvitationCount returns the number of invitations in the backend.
func InvitationCount(b *InMemoryBackend) int {
	b.mu.RLock("InvitationCount")
	defer b.mu.RUnlock()

	return len(b.invitations)
}

// AssociationCount returns the total number of associations stored in the backend.
func AssociationCount(b *InMemoryBackend) int {
	b.mu.RLock("AssociationCount")
	defer b.mu.RUnlock()

	return len(b.associations)
}

// SharePermissionCount returns the number of share->permission mappings.
func SharePermissionCount(b *InMemoryBackend) int {
	b.mu.RLock("SharePermissionCount")
	defer b.mu.RUnlock()

	total := 0
	for _, m := range b.sharePermissions {
		total += len(m)
	}

	return total
}

// HandlerOpsLen returns the number of operations listed in GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// AddResourceShareInternal inserts a resource share directly, bypassing validation.
// Useful for seeding test state.
func AddResourceShareInternal(b *InMemoryBackend, rs *ResourceShare) {
	b.AddResourceShareInternal(rs)
}

// AddPermissionInternal inserts a permission directly, bypassing validation.
// Useful for seeding test state.
func AddPermissionInternal(b *InMemoryBackend, p *Permission) {
	b.AddPermissionInternal(p)
}

// AddInvitationInternal adds a pre-built invitation for testing or seeding.
func AddInvitationInternal(b *InMemoryBackend, inv *ResourceShareInvitation) {
	b.mu.Lock("AddInvitationInternal")
	defer b.mu.Unlock()

	b.invitations[inv.InvitationARN] = inv
}

// NewTestResourceShare is a convenience helper that builds a minimal ResourceShare
// for use in tests without having to set every field.
func NewTestResourceShare(arn, name string) *ResourceShare {
	now := time.Now()

	return &ResourceShare{
		ARN:             arn,
		Name:            name,
		OwningAccountID: "000000000000",
		Status:          statusActive,
		Tags:            make(map[string]string),
		CreationTime:    now,
		LastUpdatedTime: now,
	}
}

// NewTestPermission is a convenience helper that builds a minimal Permission
// with a single version for use in tests.
func NewTestPermission(arn, name, resourceType string) *Permission {
	now := time.Now()
	pv := &PermissionVersion{
		Version:         1,
		PolicyTemplate:  `{"Effect":"Allow","Action":["*"]}`,
		CreationTime:    now,
		LastUpdatedTime: now,
	}

	return &Permission{
		ARN:             arn,
		Name:            name,
		ResourceType:    resourceType,
		Tags:            make(map[string]string),
		Versions:        map[int32]*PermissionVersion{1: pv},
		LatestVersion:   1,
		DefaultVersion:  1,
		CreationTime:    now,
		LastUpdatedTime: now,
	}
}

// CreateInvitation creates a pending invitation via the backend for testing purposes.
// This exposes the concrete method (not part of StorageBackend interface) to test code.
func CreateInvitation(
	b *InMemoryBackend,
	shareARN, shareNm, senderAcctID, receiverAcctID string,
) *ResourceShareInvitation {
	return b.CreateInvitation(shareARN, shareNm, senderAcctID, receiverAcctID)
}
func NewTestInvitation(invARN, shareARN, shareName string) *ResourceShareInvitation {
	now := time.Now()

	return &ResourceShareInvitation{
		InvitationARN:     invARN,
		ResourceShareARN:  shareARN,
		ResourceShareName: shareName,
		SenderAccountID:   "111111111111",
		ReceiverAccountID: "000000000000",
		Status:            invitationStatusPending,
		CreationTime:      now,
		LastUpdatedTime:   now,
	}
}
