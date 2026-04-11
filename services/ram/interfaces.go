package ram

// StorageBackend defines the interface for the AWS RAM in-memory backend.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Resource share operations
	CreateResourceShare(
		name string,
		allowExternalPrincipals bool,
		tags map[string]string,
		principals, resourceARNs []string,
	) (*ResourceShare, error)
	GetResourceShare(shareARN string) (*ResourceShare, error)
	ListResourceShares(resourceOwner string) []*ResourceShare
	UpdateResourceShare(shareARN, name string, allowExternalPrincipals *bool) (*ResourceShare, error)
	DeleteResourceShare(shareARN string) error
	AssociateResourceShare(shareARN string, principals, resourceARNs []string) ([]*ResourceShareAssociation, error)
	DisassociateResourceShare(shareARN string, principals, resourceARNs []string) ([]*ResourceShareAssociation, error)
	GetResourceShareAssociations(assocType string, shareARNs []string) []*ResourceShareAssociation

	// Tag operations
	TagResource(shareARN string, tags map[string]string) error
	UntagResource(shareARN string, tagKeys []string) error
	ListTagsForResource(shareARN string) (map[string]string, error)

	// Permission operations
	CreatePermission(name, resourceType, policyTemplate string, tags map[string]string) (*Permission, error)
	CreatePermissionVersion(permissionARN, policyTemplate string) (*Permission, error)
	DeletePermission(permissionARN string) error
	DeletePermissionVersion(permissionARN string, permissionVersion int32) error
	GetPermission(permissionARN string, permissionVersion *int32) (*Permission, *PermissionVersion, error)
	AssociateResourceSharePermission(shareARN, permissionARN string, replace bool, permissionVersion *int32) error
	DisassociateResourceSharePermission(shareARN, permissionARN string) error

	// Invitation operations
	AcceptResourceShareInvitation(invitationARN string) (*ResourceShareInvitation, error)
	GetResourceShareInvitations(invitationARNs, shareARNs []string) []*ResourceShareInvitation
	CreateInvitation(shareARN, shareNm, senderAcctID, receiverAcctID string) *ResourceShareInvitation
	AddInvitationInternal(inv *ResourceShareInvitation)

	// Resource policy operations
	GetResourcePolicies(resourceARNs []string) []string

	// Persistence
	Snapshot() []byte
	Restore(data []byte) error

	// Lifecycle
	Reset()
	AccountID() string
	Region() string
}

// Verify at compile time that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
