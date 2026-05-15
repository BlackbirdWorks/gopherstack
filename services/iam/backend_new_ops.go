package iam

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---- Account Alias ----

// CreateAccountAlias creates an account alias for the AWS account.
// In real AWS, at most one account alias may exist; this implementation allows replacing it.
func (b *InMemoryBackend) CreateAccountAlias(alias string) error {
	if alias == "" {
		return fmt.Errorf("%w: account alias must not be empty", ErrInvalidAction)
	}

	b.mu.Lock("CreateAccountAlias")
	defer b.mu.Unlock()

	b.accountAliases = []string{alias}

	return nil
}

// ---- Policy Versions ----

// CreatePolicyVersion creates a new version of an existing managed policy.
// Up to five versions can exist at once (AWS limit); this implementation enforces it.
// If setAsDefault is true, the new version becomes the default (and the policy document is updated).
func (b *InMemoryBackend) CreatePolicyVersion(
	policyArn, policyDocument string, setAsDefault bool,
) (*StoredPolicyVersion, error) {
	if policyDocument == "" {
		return nil, fmt.Errorf("%w: policy document must not be empty", ErrMalformedPolicyDocument)
	}

	b.mu.Lock("CreatePolicyVersion")
	defer b.mu.Unlock()

	polName, exists := b.policyByARN[policyArn]
	if !exists {
		return nil, fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyArn)
	}

	pol, exists := b.policies[polName]
	if !exists {
		return nil, fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyArn)
	}

	versions := b.policyVersions[policyArn]

	// AWS allows at most 5 versions per policy.
	const maxPolicyVersions = 5
	if len(versions) >= maxPolicyVersions {
		return nil, fmt.Errorf(
			"%w: policy %q already has %d versions; delete one before creating another",
			ErrDeleteConflict, policyArn, maxPolicyVersions,
		)
	}

	// v1 is the original; new versions start at v2.
	const firstNewVersionOffset = 2
	nextVersionNum := len(versions) + firstNewVersionOffset
	versionID := fmt.Sprintf("v%d", nextVersionNum)

	newVersion := StoredPolicyVersion{
		VersionID:        versionID,
		PolicyDocument:   policyDocument,
		IsDefaultVersion: setAsDefault,
		CreateDate:       time.Now().UTC(),
	}

	if setAsDefault {
		// Clear default flag on all existing versions.
		for i := range versions {
			versions[i].IsDefaultVersion = false
		}

		// Update the policy document on the policy itself.
		pol.PolicyDocument = policyDocument
		b.policies[polName] = pol
	}

	versions = append(versions, newVersion)
	b.policyVersions[policyArn] = versions

	return &newVersion, nil
}

// ListPolicyVersions returns all stored versions for a managed policy, including v1.
func (b *InMemoryBackend) ListPolicyVersions(policyArn string) ([]StoredPolicyVersion, error) {
	b.mu.RLock("ListPolicyVersions")
	defer b.mu.RUnlock()

	policy, exists := b.getPolicyByARNLocked(policyArn)
	if !exists {
		return nil, fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyArn)
	}

	storedVersions := b.policyVersions[policyArn]
	isV1Default := true
	for _, storedVersion := range storedVersions {
		if storedVersion.IsDefaultVersion {
			isV1Default = false

			break
		}
	}

	versions := make([]StoredPolicyVersion, 0, len(storedVersions)+1)

	if !b.deletedV1Policies[policyArn] {
		versions = append(versions, StoredPolicyVersion{
			VersionID:        "v1",
			PolicyDocument:   policy.PolicyDocument,
			CreateDate:       policy.CreateDate,
			IsDefaultVersion: isV1Default,
		})
	}

	return append(versions, storedVersions...), nil
}

// ---- Service-Linked Roles ----

// CreateServiceLinkedRole creates an IAM role that is linked to a specific AWS service.
// The role name is derived from the service name and optional custom suffix.
func (b *InMemoryBackend) CreateServiceLinkedRole(
	awsServiceName, description, customSuffix string,
) (*Role, error) {
	if awsServiceName == "" {
		return nil, fmt.Errorf("%w: AWSServiceName must not be empty", ErrInvalidAction)
	}

	// Build a role name from the service name.
	// e.g., elasticloadbalancing.amazonaws.com → AWSServiceRoleForElasticLoadBalancing
	svcShort := strings.TrimSuffix(awsServiceName, ".amazonaws.com")
	roleName := "AWSServiceRoleFor" + strings.ToUpper(svcShort[:1]) + svcShort[1:]
	if customSuffix != "" {
		roleName = roleName + "_" + customSuffix
	}

	const svcLinkedPath = "/aws-service-role/"

	b.mu.Lock("CreateServiceLinkedRole")
	defer b.mu.Unlock()

	if _, exists := b.roles[roleName]; exists {
		return nil, fmt.Errorf("%w: role %q already exists", ErrRoleAlreadyExists, roleName)
	}

	assumeRolePolicy := fmt.Sprintf(
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":%q},"Action":"sts:AssumeRole"}]}`,
		awsServiceName,
	)

	p := normPath(svcLinkedPath + awsServiceName + "/")
	role := Role{
		RoleName:                 roleName,
		RoleID:                   "AROA" + strings.ToUpper(uuid.New().String()[:16]),
		Arn:                      arn.Build("iam", "", b.accountID, "role"+p+roleName),
		Path:                     p,
		AssumeRolePolicyDocument: assumeRolePolicy,
		CreateDate:               time.Now().UTC(),
	}

	if description != "" {
		// Store description as a tag; IAM roles do not have a description field here.
		_ = description
	}

	b.roles[roleName] = role
	b.roleByARN[role.Arn] = roleName

	return &role, nil
}

// ---- Service-Specific Credentials ----

// CreateServiceSpecificCredential creates service-specific credentials for an IAM user.
func (b *InMemoryBackend) CreateServiceSpecificCredential(
	userName, serviceName string,
) (*ServiceSpecificCredential, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("%w: ServiceName must not be empty", ErrInvalidAction)
	}

	b.mu.Lock("CreateServiceSpecificCredential")
	defer b.mu.Unlock()

	if _, exists := b.users[userName]; !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	credID := "ACCAI" + strings.ToUpper(uuid.New().String()[:16])
	svcUserName := userName + "+" + serviceName
	svcPassword := uuid.New().String() + uuid.New().String()

	cred := ServiceSpecificCredential{
		ServiceSpecificCredentialID: credID,
		UserName:                    userName,
		ServiceName:                 serviceName,
		ServiceUserName:             svcUserName,
		ServicePassword:             svcPassword,
		Status:                      "Active",
		CreateDate:                  time.Now().UTC(),
	}

	b.serviceSpecificCreds[credID] = cred

	return &cred, nil
}

// ---- Virtual MFA Devices ----

// CreateVirtualMFADevice creates a virtual MFA device.
func (b *InMemoryBackend) CreateVirtualMFADevice(virtualMFADeviceName, path string) (*VirtualMFADevice, error) {
	if virtualMFADeviceName == "" {
		return nil, fmt.Errorf("%w: VirtualMFADeviceName must not be empty", ErrInvalidAction)
	}

	p := normPath(path)
	serialNumber := arn.Build("iam", "", b.accountID, "mfa"+p+virtualMFADeviceName)

	b.mu.Lock("CreateVirtualMFADevice")
	defer b.mu.Unlock()

	if _, exists := b.virtualMFADevices[serialNumber]; exists {
		return nil, fmt.Errorf("%w: virtual MFA device %q already exists", ErrUserAlreadyExists, virtualMFADeviceName)
	}

	device := VirtualMFADevice{
		SerialNumber:         serialNumber,
		VirtualMFADeviceName: virtualMFADeviceName,
		Path:                 p,
		CreateDate:           time.Now().UTC(),
		Status:               MFAStatusNotAssigned,
	}

	b.virtualMFADevices[serialNumber] = device

	return &device, nil
}

// ---- Delegation Requests ----

// CreateDelegationRequest creates a delegation request (stub implementation).
func (b *InMemoryBackend) CreateDelegationRequest(targetAccountID string) (*DelegationRequest, error) {
	delegationID := uuid.New().String()

	b.mu.Lock("CreateDelegationRequest")
	defer b.mu.Unlock()

	req := DelegationRequest{
		DelegationID:    delegationID,
		TargetAccountID: targetAccountID,
		Status:          "PENDING",
		CreateDate:      time.Now().UTC(),
	}

	b.delegationRequests[delegationID] = req

	return &req, nil
}

// AcceptDelegationRequest accepts a delegation request (stub implementation).
func (b *InMemoryBackend) AcceptDelegationRequest(delegationID string) error {
	b.mu.Lock("AcceptDelegationRequest")
	defer b.mu.Unlock()

	req, exists := b.delegationRequests[delegationID]
	if !exists {
		return fmt.Errorf("%w: delegation request %q not found", ErrInvalidAction, delegationID)
	}

	req.Status = "ACCEPTED"
	b.delegationRequests[delegationID] = req

	return nil
}

// AssociateDelegationRequest associates a delegation request with a policy ARN (stub implementation).
func (b *InMemoryBackend) AssociateDelegationRequest(delegationID, policyArn string) error {
	b.mu.Lock("AssociateDelegationRequest")
	defer b.mu.Unlock()

	req, exists := b.delegationRequests[delegationID]
	if !exists {
		return fmt.Errorf("%w: delegation request %q not found", ErrInvalidAction, delegationID)
	}

	req.PolicyArn = policyArn
	b.delegationRequests[delegationID] = req

	return nil
}

// ---- Change Password ----

// ChangePassword changes the IAM user password (mock: validates that the new password is not empty).
// In real AWS, this operates on the currently authenticated user; in this mock, any non-empty
// new password is accepted.
func (b *InMemoryBackend) ChangePassword(newPassword string) error {
	if newPassword == "" {
		return fmt.Errorf("%w: new password must not be empty", ErrInvalidPassword)
	}

	return nil
}

// ---- OIDC Client IDs ----

// AddClientIDToOpenIDConnectProvider appends a client ID to an existing OIDC provider.
// If the client ID is already present, the call is idempotent.
func (b *InMemoryBackend) AddClientIDToOpenIDConnectProvider(providerArn, clientID string) error {
	if clientID == "" {
		return fmt.Errorf("%w: ClientID must not be empty", ErrInvalidAction)
	}

	b.mu.Lock("AddClientIDToOpenIDConnectProvider")
	defer b.mu.Unlock()

	p, exists := b.oidcProviders[providerArn]
	if !exists {
		return fmt.Errorf("%w: OIDC provider %q not found", ErrOIDCProviderNotFound, providerArn)
	}

	if !slices.Contains(p.ClientIDList, clientID) {
		p.ClientIDList = append(p.ClientIDList, clientID)
		b.oidcProviders[providerArn] = p
	}

	return nil
}
