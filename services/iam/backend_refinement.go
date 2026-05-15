package iam

import (
	"fmt"
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ---- Access Key Updates ----

// UpdateAccessKey updates the status of an access key (Active or Inactive).
func (b *InMemoryBackend) UpdateAccessKey(userName, accessKeyID, status string) error {
	if status != accessKeyStatusActive && status != "Inactive" {
		return fmt.Errorf("%w: status must be Active or Inactive", ErrInvalidAction)
	}

	b.mu.Lock("UpdateAccessKey")
	defer b.mu.Unlock()

	ak, exists := b.accessKeys[accessKeyID]
	if !exists || ak.UserName != userName {
		return fmt.Errorf("%w: access key %q not found for user %q", ErrAccessKeyNotFound, accessKeyID, userName)
	}

	ak.Status = status
	b.accessKeys[accessKeyID] = ak

	return nil
}

// GetAccessKeyLastUsed returns last-use information for an access key.
// Returns real data if the key has been used (via RecordAccessKeyUsage); otherwise returns N/A.
func (b *InMemoryBackend) GetAccessKeyLastUsed(accessKeyID string) (*AccessKeyLastUsed, error) {
	b.mu.RLock("GetAccessKeyLastUsed")
	defer b.mu.RUnlock()

	ak, exists := b.accessKeys[accessKeyID]
	if !exists {
		return nil, fmt.Errorf("%w: access key %q not found", ErrAccessKeyNotFound, accessKeyID)
	}

	result := &AccessKeyLastUsed{
		UserName:    ak.UserName,
		AccessKeyID: accessKeyID,
	}

	if ak.LastUsedDate != nil {
		result.LastUsedDate = ak.LastUsedDate.UTC().Format("2006-01-02T15:04:05Z")
		result.ServiceName = ak.LastUsedServiceName
		result.Region = ak.LastUsedRegion
	} else {
		result.LastUsedDate = notApplicable
		result.ServiceName = notApplicable
		result.Region = notApplicable
	}

	return result, nil
}

// ---- Account Aliases ----

// ListAccountAliases returns the current account aliases.
func (b *InMemoryBackend) ListAccountAliases() []string {
	b.mu.RLock("ListAccountAliases")
	defer b.mu.RUnlock()

	if len(b.accountAliases) == 0 {
		return []string{}
	}

	result := make([]string, len(b.accountAliases))
	copy(result, b.accountAliases)

	return result
}

// DeleteAccountAlias removes the specified account alias.
func (b *InMemoryBackend) DeleteAccountAlias(alias string) error {
	b.mu.Lock("DeleteAccountAlias")
	defer b.mu.Unlock()

	for i, a := range b.accountAliases {
		if a == alias {
			b.accountAliases = append(b.accountAliases[:i], b.accountAliases[i+1:]...)

			return nil
		}
	}

	return fmt.Errorf("%w: account alias %q not found", ErrInvalidAction, alias)
}

// ---- Groups For User ----

// ListGroupsForUser returns all groups that the specified user belongs to.
func (b *InMemoryBackend) ListGroupsForUser(userName string) ([]Group, error) {
	b.mu.RLock("ListGroupsForUser")
	defer b.mu.RUnlock()

	if _, exists := b.users[userName]; !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	result := make([]Group, 0)
	for groupName, members := range b.groupMembers {
		if slices.Contains(members, userName) {
			if g, exists := b.groups[groupName]; exists {
				result = append(result, g)
			}
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i].GroupName < result[j].GroupName })

	return result, nil
}

// ---- Virtual MFA Devices ----

// ListVirtualMFADevices returns a paginated list of virtual MFA devices.
func (b *InMemoryBackend) ListVirtualMFADevices(marker string, maxItems int) (page.Page[VirtualMFADevice], error) {
	b.mu.RLock("ListVirtualMFADevices")
	defer b.mu.RUnlock()

	devices := make([]VirtualMFADevice, 0, len(b.virtualMFADevices))
	for _, d := range b.virtualMFADevices {
		devices = append(devices, d)
	}

	sort.Slice(devices, func(i, j int) bool {
		return devices[i].SerialNumber < devices[j].SerialNumber
	})

	return page.New(devices, marker, maxItems, iamDefaultMaxItems), nil
}

// DeleteVirtualMFADevice deletes a virtual MFA device by its serial number.
func (b *InMemoryBackend) DeleteVirtualMFADevice(serialNumber string) error {
	b.mu.Lock("DeleteVirtualMFADevice")
	defer b.mu.Unlock()

	if _, exists := b.virtualMFADevices[serialNumber]; !exists {
		return fmt.Errorf("%w: virtual MFA device %q not found", ErrUserNotFound, serialNumber)
	}

	delete(b.virtualMFADevices, serialNumber)

	return nil
}

// ---- Policy Versions (set default, delete) ----

// SetDefaultPolicyVersion sets the specified version as the default for the managed policy.
func (b *InMemoryBackend) SetDefaultPolicyVersion(policyArn, versionID string) error {
	b.mu.Lock("SetDefaultPolicyVersion")
	defer b.mu.Unlock()

	polName, exists := b.policyByARN[policyArn]
	if !exists {
		return fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyArn)
	}

	if versionID == "v1" {
		// v1 is default when no stored version overrides it.
		versions := b.policyVersions[policyArn]
		for i := range versions {
			versions[i].IsDefaultVersion = false
		}
		b.policyVersions[policyArn] = versions

		return nil
	}

	versions := b.policyVersions[policyArn]
	found := false

	for i := range versions {
		if versions[i].VersionID == versionID {
			found = true
			versions[i].IsDefaultVersion = true

			// Update the policy's document to the new default.
			pol := b.policies[polName]
			pol.PolicyDocument = versions[i].PolicyDocument
			b.policies[polName] = pol
		} else {
			versions[i].IsDefaultVersion = false
		}
	}

	if !found {
		return fmt.Errorf("%w: policy version %q not found for policy %q", ErrPolicyNotFound, versionID, policyArn)
	}

	b.policyVersions[policyArn] = versions

	return nil
}

// DeletePolicyVersion deletes a non-default version of the managed policy.
func (b *InMemoryBackend) DeletePolicyVersion(policyArn, versionID string) error {
	b.mu.Lock("DeletePolicyVersion")
	defer b.mu.Unlock()

	if _, exists := b.policyByARN[policyArn]; !exists {
		return fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyArn)
	}

	// v1 is stored implicitly. If being deleted, verify it is not the current default.
	if versionID == "v1" {
		nonV1IsDefault := false

		for _, v := range b.policyVersions[policyArn] {
			if v.IsDefaultVersion {
				nonV1IsDefault = true

				break
			}
		}

		if !nonV1IsDefault {
			// v1 is still the default; cannot delete.
			return fmt.Errorf("%w: cannot delete the default version v1", ErrDeleteConflict)
		}

		// Mark v1 as deleted for this policy.
		b.deletedV1Policies[policyArn] = true

		return nil
	}

	versions := b.policyVersions[policyArn]
	for i, v := range versions {
		if v.VersionID != versionID {
			continue
		}

		if v.IsDefaultVersion {
			return fmt.Errorf("%w: cannot delete the default policy version %q", ErrDeleteConflict, versionID)
		}

		b.policyVersions[policyArn] = append(versions[:i], versions[i+1:]...)

		return nil
	}

	return fmt.Errorf("%w: policy version %q not found for policy %q", ErrPolicyNotFound, versionID, policyArn)
}

// defaultMinPasswordLength is the default minimum password length for the account password policy.
const defaultMinPasswordLength = 8

// GetAccountPasswordPolicy returns the current account password policy.
// Returns a default strict policy when none has been set.
func (b *InMemoryBackend) GetAccountPasswordPolicy() *PasswordPolicy {
	b.mu.RLock("GetAccountPasswordPolicy")
	defer b.mu.RUnlock()

	if b.passwordPolicy != nil {
		pp := *b.passwordPolicy

		return &pp
	}

	return defaultPasswordPolicy()
}

// UpdateAccountPasswordPolicy stores the account password policy.
func (b *InMemoryBackend) UpdateAccountPasswordPolicy(pp PasswordPolicy) error {
	b.mu.Lock("UpdateAccountPasswordPolicy")
	defer b.mu.Unlock()

	b.passwordPolicy = &pp

	return nil
}

// DeleteAccountPasswordPolicy removes the account password policy (resets to default).
func (b *InMemoryBackend) DeleteAccountPasswordPolicy() error {
	b.mu.Lock("DeleteAccountPasswordPolicy")
	defer b.mu.Unlock()

	if b.passwordPolicy == nil {
		return fmt.Errorf("%w: no account password policy set", ErrPolicyNotFound)
	}

	b.passwordPolicy = nil

	return nil
}

func defaultPasswordPolicy() *PasswordPolicy {
	return &PasswordPolicy{
		MinimumPasswordLength:      defaultMinPasswordLength,
		RequireUppercaseCharacters: false,
		RequireLowercaseCharacters: false,
		RequireNumbers:             false,
		RequireSymbols:             false,
		AllowUsersToChangePassword: true,
		MaxPasswordAge:             0,
		PasswordReusePrevention:    0,
		HardExpiry:                 false,
	}
}

// ---- List Entities For Policy ----

// ListEntitiesForPolicy returns the users, groups, and roles that have the specified policy attached.
func (b *InMemoryBackend) ListEntitiesForPolicy(policyArn, entityFilter string) (*PolicyEntities, error) {
	b.mu.RLock("ListEntitiesForPolicy")
	defer b.mu.RUnlock()

	if _, exists := b.getPolicyByARNLocked(policyArn); !exists {
		return nil, fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyArn)
	}

	refs := b.policyAttachments[policyArn]
	result := &PolicyEntities{}

	if entityFilter == "" || entityFilter == "User" {
		for userName := range refs.users {
			result.PolicyUsers = append(result.PolicyUsers, PolicyEntityUser{UserName: userName})
		}

		sort.Slice(result.PolicyUsers, func(i, j int) bool {
			return result.PolicyUsers[i].UserName < result.PolicyUsers[j].UserName
		})
	}

	if entityFilter == "" || entityFilter == "Group" {
		for groupName := range refs.groups {
			result.PolicyGroups = append(result.PolicyGroups, PolicyEntityGroup{GroupName: groupName})
		}

		sort.Slice(result.PolicyGroups, func(i, j int) bool {
			return result.PolicyGroups[i].GroupName < result.PolicyGroups[j].GroupName
		})
	}

	if entityFilter == "" || entityFilter == "Role" {
		for roleName := range refs.roles {
			result.PolicyRoles = append(result.PolicyRoles, PolicyEntityRole{RoleName: roleName})
		}

		sort.Slice(result.PolicyRoles, func(i, j int) bool {
			return result.PolicyRoles[i].RoleName < result.PolicyRoles[j].RoleName
		})
	}

	return result, nil
}

// ---- Service-Specific Credentials ----

// ListServiceSpecificCredentials returns service-specific credentials for a user.
// If serviceName is non-empty, only credentials for that service are returned.
func (b *InMemoryBackend) ListServiceSpecificCredentials(
	userName, serviceName string,
) ([]ServiceSpecificCredential, error) {
	b.mu.RLock("ListServiceSpecificCredentials")
	defer b.mu.RUnlock()

	if _, exists := b.users[userName]; !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	result := make([]ServiceSpecificCredential, 0)
	for _, cred := range b.serviceSpecificCreds {
		if cred.UserName != userName {
			continue
		}

		if serviceName != "" && cred.ServiceName != serviceName {
			continue
		}

		result = append(result, cred)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ServiceSpecificCredentialID < result[j].ServiceSpecificCredentialID
	})

	return result, nil
}

// DeleteServiceSpecificCredential deletes a service-specific credential.
func (b *InMemoryBackend) DeleteServiceSpecificCredential(userName, credentialID string) error {
	b.mu.Lock("DeleteServiceSpecificCredential")
	defer b.mu.Unlock()

	cred, exists := b.serviceSpecificCreds[credentialID]
	if !exists || cred.UserName != userName {
		return fmt.Errorf("%w: credential %q not found for user %q", ErrAccessKeyNotFound, credentialID, userName)
	}

	delete(b.serviceSpecificCreds, credentialID)

	return nil
}

// ---- Update User ----

// UpdateUser renames a user and/or updates their path.
// If newUserName is non-empty the user is renamed; if newPath is non-empty the path is updated.
func (b *InMemoryBackend) UpdateUser(userName, newPath, newUserName string) error {
	b.mu.Lock("UpdateUser")
	defer b.mu.Unlock()

	u, ok := b.users[userName]
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if newUserName != "" && newUserName != userName {
		if _, taken := b.users[newUserName]; taken {
			return fmt.Errorf("%w: user %q already exists", ErrUserAlreadyExists, newUserName)
		}
	}

	if newPath != "" {
		u.Path = normPath(newPath)
	}

	if newUserName != "" && newUserName != userName {
		b.migrateUserData(userName, newUserName)
		u.UserName = newUserName
		delete(b.users, userName)
	}

	b.users[u.UserName] = u

	return nil
}

// migrateUserData moves per-user maps to the new name (called with lock held).
func (b *InMemoryBackend) migrateUserData(oldName, newName string) {
	for id, ak := range b.accessKeys {
		if ak.UserName == oldName {
			ak.UserName = newName
			b.accessKeys[id] = ak
		}
	}

	if lp, found := b.loginProfiles[oldName]; found {
		b.loginProfiles[newName] = lp
		delete(b.loginProfiles, oldName)
	}

	if policies, found := b.userPolicies[oldName]; found {
		b.userPolicies[newName] = policies
		delete(b.userPolicies, oldName)
	}

	if inline, found := b.userInlinePolicies[oldName]; found {
		b.userInlinePolicies[newName] = inline
		delete(b.userInlinePolicies, oldName)
	}

	for groupName, members := range b.groupMembers {
		for i, m := range members {
			if m == oldName {
				b.groupMembers[groupName][i] = newName
			}
		}
	}
}

// ---- Update Role ----

// UpdateRole updates the description of an IAM role.
func (b *InMemoryBackend) UpdateRole(roleName, description string) error {
	b.mu.Lock("UpdateRole")
	defer b.mu.Unlock()

	r, exists := b.roles[roleName]
	if !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	r.Description = description
	b.roles[roleName] = r

	return nil
}

// ---- Update Group ----

// UpdateGroup renames a group and/or updates its path.
func (b *InMemoryBackend) UpdateGroup(groupName, newPath, newGroupName string) error {
	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	g, ok := b.groups[groupName]
	if !ok {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if newGroupName != "" && newGroupName != groupName {
		if _, taken := b.groups[newGroupName]; taken {
			return fmt.Errorf("%w: group %q already exists", ErrGroupAlreadyExists, newGroupName)
		}
	}

	if newPath != "" {
		g.Path = normPath(newPath)
	}

	if newGroupName != "" && newGroupName != groupName {
		b.migrateGroupData(groupName, newGroupName)
		g.GroupName = newGroupName
		delete(b.groups, groupName)
	}

	b.groups[g.GroupName] = g

	return nil
}

// migrateGroupData moves per-group maps to the new name (called with lock held).
func (b *InMemoryBackend) migrateGroupData(oldName, newName string) {
	if policies, found := b.groupPolicies[oldName]; found {
		b.groupPolicies[newName] = policies
		delete(b.groupPolicies, oldName)
	}

	if members, found := b.groupMembers[oldName]; found {
		b.groupMembers[newName] = members
		delete(b.groupMembers, oldName)
	}

	if inline, found := b.groupInlinePolicies[oldName]; found {
		b.groupInlinePolicies[newName] = inline
		delete(b.groupInlinePolicies, oldName)
	}
}

// ---- OIDC Provider Client IDs ----

// RemoveClientIDFromOpenIDConnectProvider removes a client ID from an OIDC provider.
func (b *InMemoryBackend) RemoveClientIDFromOpenIDConnectProvider(providerArn, clientID string) error {
	b.mu.Lock("RemoveClientIDFromOpenIDConnectProvider")
	defer b.mu.Unlock()

	p, exists := b.oidcProviders[providerArn]
	if !exists {
		return fmt.Errorf("%w: OIDC provider %q not found", ErrOIDCProviderNotFound, providerArn)
	}

	for i, id := range p.ClientIDList {
		if id == clientID {
			p.ClientIDList = append(p.ClientIDList[:i], p.ClientIDList[i+1:]...)
			b.oidcProviders[providerArn] = p

			return nil
		}
	}

	return fmt.Errorf("%w: client ID %q not found in OIDC provider %q", ErrInvalidAction, clientID, providerArn)
}

// ---- Instance Profiles ----

// GetInstanceProfile retrieves a single IAM instance profile by name.
func (b *InMemoryBackend) GetInstanceProfile(name string) (*InstanceProfile, error) {
	b.mu.RLock("GetInstanceProfile")
	defer b.mu.RUnlock()

	ip, exists := b.instanceProfiles[name]
	if !exists {
		return nil, fmt.Errorf("%w: instance profile %q not found", ErrInstanceProfileNotFound, name)
	}

	return &ip, nil
}

// ListInstanceProfilesForRole returns all instance profiles that contain the specified role.
func (b *InMemoryBackend) ListInstanceProfilesForRole(roleName string) ([]InstanceProfile, error) {
	b.mu.RLock("ListInstanceProfilesForRole")
	defer b.mu.RUnlock()

	if _, exists := b.roles[roleName]; !exists {
		return nil, fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	result := make([]InstanceProfile, 0)
	for _, ip := range b.instanceProfiles {
		if slices.Contains(ip.Roles, roleName) {
			result = append(result, ip)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].InstanceProfileName < result[j].InstanceProfileName
	})

	return result, nil
}

// ---- Simulate Custom Policy ----

// SimulateCustomPolicy simulates the effect of one or more custom IAM policies against a set of actions and resources.
// This is a best-effort simulation — results are authoritative only for policies provided directly.
func (b *InMemoryBackend) SimulateCustomPolicy(
	policyInputList, actionNames, resourceArns []string,
) ([]SimulationResult, error) {
	if len(actionNames) == 0 {
		return nil, fmt.Errorf("%w: at least one action name is required", ErrInvalidAction)
	}

	b.mu.RLock("SimulateCustomPolicy")
	defer b.mu.RUnlock()

	if len(resourceArns) == 0 {
		resourceArns = []string{"*"}
	}

	results := make([]SimulationResult, 0, len(actionNames)*len(resourceArns))

	for _, action := range actionNames {
		for _, resource := range resourceArns {
			evalResult := EvaluatePolicies(policyInputList, action, resource, ConditionContext{})

			var decision string

			switch evalResult {
			case EvalAllow:
				decision = "allowed"
			case EvalExplicitDeny:
				decision = "explicitDeny"
			default:
				decision = "implicitDeny"
			}

			results = append(results, SimulationResult{
				ActionName:   action,
				ResourceName: resource,
				Decision:     decision,
			})
		}
	}

	return results, nil
}

// ---- GetServiceLinkedRoleDeletionStatus (stub) ----

// GetServiceLinkedRoleDeletionStatus returns the status of a service-linked role deletion task.
// Gopherstack does not implement asynchronous deletion; this stub always reports succeeded.
func (b *InMemoryBackend) GetServiceLinkedRoleDeletionStatus(deletionTaskID string) (string, error) {
	if deletionTaskID == "" {
		return "", fmt.Errorf("%w: DeletionTaskId must not be empty", ErrInvalidAction)
	}

	return "SUCCEEDED", nil
}
