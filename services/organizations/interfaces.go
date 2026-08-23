package organizations

import (
	"context"
	"time"
)

// StorageBackend defines the interface for the Organizations in-memory backend.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Organization operations
	CreateOrganization(featureSet string) (*Organization, *Root, error)
	DescribeOrganization() (*Organization, error)
	DeleteOrganization() error
	EnsureOrgExists() error

	// Account operations
	CreateAccount(name, email, roleName, iamUserAccessToBilling string, tags []Tag) (*CreateAccountStatus, error)
	CreateGovCloudAccount(
		name, email, roleName, iamUserAccessToBilling string,
		tags []Tag,
	) (*CreateAccountStatus, error)
	DescribeCreateAccountStatus(requestID string) (*CreateAccountStatus, error)
	DescribeAccount(accountID string) (*Account, error)
	ListAccounts() ([]*Account, error)
	RemoveAccountFromOrganization(accountID string) error
	MoveAccount(accountID, sourceParentID, destParentID string) error
	CloseAccount(accountID string) error

	// Root operations
	ListRoots() ([]*Root, error)

	// OU operations
	CreateOrganizationalUnit(parentID, name string, tags []Tag) (*OrganizationalUnit, error)
	DescribeOrganizationalUnit(ouID string) (*OrganizationalUnit, error)
	DeleteOrganizationalUnit(ouID string) error
	UpdateOrganizationalUnit(ouID, name string) (*OrganizationalUnit, error)
	ListOrganizationalUnitsForParent(parentID string) ([]*OrganizationalUnit, error)
	ListAccountsForParent(parentID string) ([]*Account, error)
	ListParents(childID string) ([]ParentSummary, error)
	ListChildren(parentID, childType string) ([]ChildSummary, error)

	// Policy operations
	CreatePolicy(name, description, content, policyType string, tags []Tag) (*Policy, error)
	DescribePolicy(policyID string) (*Policy, error)
	UpdatePolicy(policyID, name, description, content string) (*Policy, error)
	DeletePolicy(policyID string) error
	ListPolicies(filter string) ([]*Policy, error)
	AttachPolicy(policyID, targetID string) error
	DetachPolicy(policyID, targetID string) error
	ListPoliciesForTarget(targetID, filter string) ([]*Policy, error)
	ListTargetsForPolicy(policyID string) ([]PolicyTargetSummary, error)
	EnablePolicyType(rootID, policyType string) (*Root, error)
	DisablePolicyType(rootID, policyType string) (*Root, error)

	// Tag operations
	TagResource(resourceID string, tags []Tag) error
	UntagResource(resourceID string, tagKeys []string) error
	ListTagsForResource(resourceID string) ([]Tag, error)

	// Service access operations
	EnableAWSServiceAccess(servicePrincipal string) error
	DisableAWSServiceAccess(servicePrincipal string) error
	ListAWSServiceAccessForOrganization() ([]EnabledServicePrincipal, error)

	// Delegated admin operations
	RegisterDelegatedAdministrator(accountID, servicePrincipal string) error
	DeregisterDelegatedAdministrator(accountID, servicePrincipal string) error
	ListDelegatedAdministrators(servicePrincipal string) ([]*DelegatedAdmin, error)

	// Handshake operations
	AcceptHandshake(handshakeID string) (*Handshake, error)
	CancelHandshake(handshakeID string) (*Handshake, error)
	DeclineHandshake(handshakeID string) (*Handshake, error)
	DescribeHandshake(handshakeID string) (*Handshake, error)
	DescribeResponsibilityTransfer(transferID string) (*ResponsibilityTransfer, error)
	EnableAllFeatures() (*Handshake, error)
	InviteAccountToOrganization(target HandshakeParty, notes string, tags []Tag) (*Handshake, error)
	InviteOrganizationToTransferResponsibility(
		target HandshakeParty, params TransferResponsibilityParams,
	) (*Handshake, error)
	LeaveOrganization() error
	ListHandshakesForAccount(actionTypeFilter string) ([]*Handshake, error)
	ListHandshakesForOrganization(actionTypeFilter string) ([]*Handshake, error)
	ListInboundResponsibilityTransfers(transferType, id string) ([]*ResponsibilityTransfer, error)
	ListOutboundResponsibilityTransfers(transferType string) ([]*ResponsibilityTransfer, error)
	TerminateResponsibilityTransfer(transferID string, endTimestamp *time.Time) (*ResponsibilityTransfer, error)
	UpdateResponsibilityTransfer(transferID, name string) (*ResponsibilityTransfer, error)
	AddHandshakeInternal(h *Handshake)
	AddResponsibilityTransferInternal(rt *ResponsibilityTransfer)

	// Account status operations
	ListCreateAccountStatus(states []string) ([]*CreateAccountStatus, error)
	ListAccountsWithInvalidEffectivePolicy(policyType string) ([]*Account, error)

	// Delegated service operations
	ListDelegatedServicesForAccount(accountID string) ([]DelegatedService, error)

	// Effective policy validation operations
	ListEffectivePolicyValidationErrors(policyType, targetID string) ([]any, error)

	// Resource policy operations
	DeleteResourcePolicy() error
	DescribeResourcePolicy() (*ResourcePolicy, error)
	PutResourcePolicy(content string) (*ResourcePolicy, error)

	// Effective policy operations
	DescribeEffectivePolicy(policyType, targetID string) (*EffectivePolicy, error)

	// Lifecycle
	Reset()
	AccountID() string
	Region() string
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
