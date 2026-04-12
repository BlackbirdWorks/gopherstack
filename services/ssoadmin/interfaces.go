package ssoadmin

// StorageBackend is the interface for SSO Admin storage operations.
type StorageBackend interface {
	AccountID() string
	Region() string
	CreateInstance(name, ownerAccountID, identityStoreID string) (*Instance, error)
	ListInstances() []*Instance
	DescribeInstance(instanceArn string) (*Instance, error)
	DeleteInstance(instanceArn string) error
	CreatePermissionSet(
		instanceArn, name, description, sessionDuration, relayState string,
		tags map[string]string,
	) (*PermissionSet, error)
	DescribePermissionSet(instanceArn, permissionSetArn string) (*PermissionSet, error)
	ListPermissionSets(instanceArn string) []*PermissionSet
	DeletePermissionSet(instanceArn, permissionSetArn string) error
	UpdatePermissionSet(instanceArn, permissionSetArn, description, sessionDuration, relayState string) error
	CreateAccountAssignment(instanceArn, permissionSetArn, accountID, principalType, principalID string) (string, error)
	DescribeAccountAssignmentCreationStatus(instanceArn, requestID string) (*ProvisioningStatus, error)
	ListAccountAssignments(instanceArn, permissionSetArn, accountID string) []*AccountAssignment
	DeleteAccountAssignment(instanceArn, permissionSetArn, accountID, principalType, principalID string) (string, error)
	DescribeAccountAssignmentDeletionStatus(instanceArn, requestID string) (*ProvisioningStatus, error)
	AttachManagedPolicyToPermissionSet(instanceArn, permissionSetArn, managedPolicyARN, name string) error
	DetachManagedPolicyFromPermissionSet(instanceArn, permissionSetArn, managedPolicyARN string) error
	ListManagedPoliciesInPermissionSet(instanceArn, permissionSetArn string) ([]ManagedPolicy, error)
	PutInlinePolicyToPermissionSet(instanceArn, permissionSetArn, inlinePolicy string) error
	GetInlinePolicyForPermissionSet(instanceArn, permissionSetArn string) (string, error)
	DeleteInlinePolicyFromPermissionSet(instanceArn, permissionSetArn string) error
	ProvisionPermissionSet(instanceArn, permissionSetArn string) (string, error)
	TagResource(instanceArn, resourceARN string, tags map[string]string) error
	UntagResource(instanceArn, resourceARN string, tagKeys []string) error
	ListTagsForResource(instanceArn, resourceARN string) (map[string]string, error)
}

var _ StorageBackend = (*InMemoryBackend)(nil)
