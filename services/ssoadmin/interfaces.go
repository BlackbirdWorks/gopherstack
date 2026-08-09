package ssoadmin

import (
	"context"
	"encoding/json"
)

// StorageBackend is the interface for SSO Admin storage operations.
type StorageBackend interface {
	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
	CreateInstance(name, ownerAccountID, identityStoreID string, tags map[string]string) (*Instance, error)
	ListInstances() []*Instance
	DescribeInstance(instanceArn string) (*Instance, error)
	DeleteInstance(instanceArn string) error
	CreatePermissionSet(
		instanceArn, name, description, sessionDuration, relayState string,
		tags map[string]string,
	) (*PermissionSet, error)
	DescribePermissionSet(instanceArn, permissionSetArn string) (*PermissionSet, error)
	ListPermissionSets(instanceArn string) []*PermissionSet
	ListPermissionSetsProvisionedToAccount(instanceArn, accountID, filterStatus string) []string
	DeletePermissionSet(instanceArn, permissionSetArn string) error
	UpdatePermissionSet(instanceArn, permissionSetArn, description, sessionDuration, relayState string) error
	CreateAccountAssignment(instanceArn, permissionSetArn, accountID, principalID, principalType string) (string, error)
	DescribeAccountAssignmentCreationStatus(instanceArn, requestID string) (*ProvisioningStatus, error)
	ListAccountAssignments(instanceArn, permissionSetArn, accountID string) []*AccountAssignment
	ListAccountAssignmentsForPrincipal(instanceArn, principalID, principalType string) []*AccountAssignment
	DeleteAccountAssignment(instanceArn, permissionSetArn, accountID, principalID, principalType string) (string, error)
	DescribeAccountAssignmentDeletionStatus(instanceArn, requestID string) (*ProvisioningStatus, error)
	AttachManagedPolicyToPermissionSet(instanceArn, permissionSetArn, managedPolicyARN, name string) error
	DetachManagedPolicyFromPermissionSet(instanceArn, permissionSetArn, managedPolicyARN string) error
	ListManagedPoliciesInPermissionSet(instanceArn, permissionSetArn string) ([]ManagedPolicy, error)
	PutInlinePolicyToPermissionSet(instanceArn, permissionSetArn, inlinePolicy string) error
	GetInlinePolicyForPermissionSet(instanceArn, permissionSetArn string) (string, error)
	DeleteInlinePolicyFromPermissionSet(instanceArn, permissionSetArn string) error
	// ProvisionPermissionSet provisions a permission set; targetType is AWS_ACCOUNT or ALL_PROVISIONED_ACCOUNTS.
	ProvisionPermissionSet(instanceArn, permissionSetArn, targetType, targetID string) (string, error)
	DescribePermissionSetProvisioningStatus(instanceArn, provisioningRequestID string) (*ProvisioningStatus, error)
	TagResource(instanceArn, resourceARN string, tags map[string]string) error
	UntagResource(instanceArn, resourceARN string, tagKeys []string) error
	ListTagsForResource(instanceArn, resourceARN string) (map[string]string, error)
	// AddRegion returns the region's status (ADDING, or the existing status if
	// already present), matching AddRegionOutput.Status.
	AddRegion(instanceArn, regionName string) (string, error)
	AttachCustomerManagedPolicyReferenceToPermissionSet(instanceArn, permissionSetArn, name, path string) error
	DeleteApplicationGrant(applicationArn, grantType string) error
	DeleteInstanceAccessControlAttributeConfiguration(instanceArn string) error
	DeleteTrustedTokenIssuer(trustedTokenIssuerArn string) error
	DescribeApplication(applicationArn string) (*Application, error)
	DescribeApplicationAssignment(applicationArn, principalID, principalType string) (*ApplicationAssignment, error)
	DescribeApplicationProvider(applicationProviderArn string) (*ApplicationProvider, error)
	DescribeInstanceAccessControlAttributeConfiguration(instanceArn string) (*ABACConfig, error)
	DescribeTrustedTokenIssuer(trustedTokenIssuerArn string) (*TrustedTokenIssuer, error)
	// GetPermissionsBoundaryForPermissionSet returns the full union-type boundary.
	GetPermissionsBoundaryForPermissionSet(instanceArn, permissionSetArn string) (*PermissionsBoundary, error)
	// ListAccountAssignmentCreationStatus accepts an optional filterStatus ("IN_PROGRESS", "SUCCEEDED", "FAILED", "").
	ListAccountAssignmentCreationStatus(instanceArn, filterStatus string) []*ProvisioningStatus
	// ListAccountAssignmentDeletionStatus accepts an optional filterStatus.
	ListAccountAssignmentDeletionStatus(instanceArn, filterStatus string) []*ProvisioningStatus
	ListApplicationAccessScopes(applicationArn string) ([]ScopeDetails, error)
	ListApplicationAssignments(applicationArn string) ([]*ApplicationAssignment, error)
	ListApplicationAuthenticationMethods(applicationArn string) ([]AuthMethod, error)
	ListApplicationGrants(applicationArn string) ([]ApplicationGrant, error)
	ListApplicationProviders() []*ApplicationProvider
	ListApplications(instanceArn string) []*Application
	// ListPermissionSetProvisioningStatus accepts an optional filterStatus.
	ListPermissionSetProvisioningStatus(instanceArn, filterStatus string) []*ProvisioningStatus
	ListRegions(instanceArn string) ([]RegionMetadata, error)
	ListTrustedTokenIssuers(instanceArn string) []*TrustedTokenIssuer
	PutApplicationAccessScope(applicationArn, scope string, authorizedTargets []string) error
	PutApplicationAssignmentConfiguration(applicationArn string, assignmentRequired bool) error
	// PutApplicationAuthenticationMethod stores a structured auth method body; authMethodType must be IAM.
	PutApplicationAuthenticationMethod(applicationArn, authMethodType string, body json.RawMessage) error
	// PutApplicationGrant stores a structured grant body; grantType must be a valid AWS grant type enum value.
	PutApplicationGrant(applicationArn, grantType string, body json.RawMessage) error
	// PutApplicationSessionConfiguration sets the UserBackgroundSessionApplicationStatus
	// (ENABLED or DISABLED) for an application. Matches the real
	// PutApplicationSessionConfigurationInput wire shape -- this is NOT a
	// session duration.
	PutApplicationSessionConfiguration(applicationArn, userBackgroundSessionStatus string) error
	// PutPermissionsBoundaryToPermissionSet accepts a union boundary
	// (ManagedPolicyArn xor CustomerManagedPolicyReference).
	PutPermissionsBoundaryToPermissionSet(
		instanceArn, permissionSetArn string, boundary *PermissionsBoundary,
	) error
	UpdateApplication(
		applicationArn, name, description, status string,
		portalOptions *PortalOptions,
	) (*Application, error)
	UpdateTrustedTokenIssuer(
		trustedTokenIssuerArn, name, issuerType string,
		cfg *TrustedTokenIssuerConfiguration,
	) (*TrustedTokenIssuer, error)
	CreateApplication(
		instanceArn, applicationProviderArn, name, description string,
		tags map[string]string,
		portalOptions *PortalOptions,
	) (*Application, error)
	CreateApplicationAssignment(applicationArn, principalID, principalType string) error
	CreateInstanceAccessControlAttributeConfiguration(instanceArn string, attributes []AccessControlAttribute) error
	CreateTrustedTokenIssuer(
		instanceArn, name, issuerType string,
		tags map[string]string,
		cfg *TrustedTokenIssuerConfiguration,
	) (*TrustedTokenIssuer, error)
	DeleteApplication(applicationArn string) error
	DeleteApplicationAccessScope(applicationArn, scope string) error
	DeleteApplicationAssignment(applicationArn, principalID, principalType string) error
	DeleteApplicationAuthenticationMethod(applicationArn, authMethodType string) error
	DeletePermissionsBoundaryFromPermissionSet(instanceArn, permissionSetArn string) error
	DetachCustomerManagedPolicyReferenceFromPermissionSet(instanceArn, permissionSetArn, name, path string) error
	GetApplicationAssignmentConfiguration(applicationArn string) (bool, error)
	GetApplicationSessionConfiguration(applicationArn string) (string, error)
	GetApplicationAccessScope(applicationArn, scope string) ([]string, error)
	GetApplicationAuthenticationMethod(applicationArn, authMethodType string) (json.RawMessage, error)
	GetApplicationGrant(applicationArn, grantType string) (json.RawMessage, error)
	ListCustomerManagedPolicyReferencesInPermissionSet(
		instanceArn, permissionSetArn string,
	) ([]CustomerManagedPolicyReference, error)
	// RemoveRegion returns the region's status (REMOVING), matching
	// RemoveRegionOutput.Status.
	RemoveRegion(instanceArn, regionName string) (string, error)
	// DescribeRegion returns metadata for a region previously added via AddRegion.
	DescribeRegion(instanceArn, regionName string) (*RegionMetadata, error)
	UpdateInstance(instanceArn, name string) error
	UpdateInstanceAccessControlAttributeConfiguration(instanceArn string, attributes []AccessControlAttribute) error
	ListAccountsForProvisionedPermissionSet(instanceArn, permissionSetArn, filterStatus string) ([]string, error)
	ListApplicationAssignmentsForPrincipal(instanceArn, principalID, principalType string) []*ApplicationAssignment
}

var _ StorageBackend = (*InMemoryBackend)(nil)
