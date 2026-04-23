package ssoadmin

// StorageBackend is the interface for SSO Admin storage operations.
type StorageBackend interface {
	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
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
	DescribePermissionSetProvisioningStatus(instanceArn, provisioningRequestID string) (*ProvisioningStatus, error)
	TagResource(instanceArn, resourceARN string, tags map[string]string) error
	UntagResource(instanceArn, resourceARN string, tagKeys []string) error
	ListTagsForResource(instanceArn, resourceARN string) (map[string]string, error)
	AddRegion(instanceArn, regionName string) error
	AttachCustomerManagedPolicyReferenceToPermissionSet(instanceArn, permissionSetArn, name, path string) error
	DeleteApplicationGrant(applicationArn, grantType string) error
	DeleteInstanceAccessControlAttributeConfiguration(instanceArn string) error
	DeleteTrustedTokenIssuer(trustedTokenIssuerArn string) error
	DescribeApplication(applicationArn string) (*Application, error)
	DescribeApplicationAssignment(applicationArn, principalID, principalType string) (*ApplicationAssignment, error)
	DescribeApplicationProvider(applicationProviderArn string) (*ApplicationProvider, error)
	DescribeInstanceAccessControlAttributeConfiguration(
		instanceArn string,
	) (*InstanceAccessControlAttributeConfiguration, error)
	DescribeTrustedTokenIssuer(trustedTokenIssuerArn string) (*TrustedTokenIssuer, error)
	GetPermissionsBoundaryForPermissionSet(instanceArn, permissionSetArn string) (string, error)
	ListAccountAssignmentCreationStatus(instanceArn string) []*ProvisioningStatus
	ListAccountAssignmentDeletionStatus(instanceArn string) []*ProvisioningStatus
	ListApplicationAccessScopes(applicationArn string) ([]string, error)
	ListApplicationAssignments(applicationArn string) ([]*ApplicationAssignment, error)
	ListApplicationAuthenticationMethods(applicationArn string) ([]string, error)
	ListApplicationGrants(applicationArn string) ([]string, error)
	ListApplicationProviders() []*ApplicationProvider
	ListApplications(instanceArn string) []*Application
	ListPermissionSetProvisioningStatus(instanceArn string) []*ProvisioningStatus
	ListTrustedTokenIssuers(instanceArn string) []*TrustedTokenIssuer
	PutApplicationAccessScope(applicationArn, scope string) error
	PutApplicationAssignmentConfiguration(applicationArn string, assignmentRequired bool) error
	PutApplicationAuthenticationMethod(applicationArn, authMethodType string) error
	PutApplicationGrant(applicationArn, grantType string) error
	PutApplicationSessionConfiguration(applicationArn, sessionDuration string) error
	PutPermissionsBoundaryToPermissionSet(instanceArn, permissionSetArn, managedPolicyArn string) error
	UpdateApplication(applicationArn, name, description, status string) (*Application, error)
	UpdateTrustedTokenIssuer(trustedTokenIssuerArn, name, issuerType string) (*TrustedTokenIssuer, error)
	CreateApplication(
		instanceArn, applicationProviderArn, name, description string,
		tags map[string]string,
	) (*Application, error)
	CreateApplicationAssignment(applicationArn, principalID, principalType string) error
	CreateInstanceAccessControlAttributeConfiguration(instanceArn string, attributes []AccessControlAttribute) error
	CreateTrustedTokenIssuer(instanceArn, name, issuerType string) (*TrustedTokenIssuer, error)
	DeleteApplication(applicationArn string) error
	DeleteApplicationAccessScope(applicationArn, scope string) error
	DeleteApplicationAssignment(applicationArn, principalID, principalType string) error
	DeleteApplicationAuthenticationMethod(applicationArn, authMethodType string) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
