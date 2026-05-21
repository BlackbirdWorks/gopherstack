package iam

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"
)

// credentialReportHeader is the CSV header for the IAM credential report.
const credentialReportHeader = "user,arn,user_creation_time,password_enabled,password_last_used," +
	"password_last_changed,password_next_rotation,mfa_active," +
	"access_key_1_active,access_key_1_last_rotated,access_key_1_last_used_date," +
	"access_key_1_last_used_region,access_key_1_last_used_service," +
	"access_key_2_active,access_key_2_last_rotated,access_key_2_last_used_date," +
	"access_key_2_last_used_region,access_key_2_last_used_service," +
	"cert_1_active,cert_1_last_rotated,cert_2_active,cert_2_last_rotated"

// sortedRoles returns all roles as a slice sorted by RoleName.
func sortedRoles(m map[string]Role) []Role {
	roles := make([]Role, 0, len(m))
	for _, r := range m {
		roles = append(roles, r)
	}

	sort.Slice(roles, func(i, j int) bool { return roles[i].RoleName < roles[j].RoleName })

	return roles
}

// sortedGroups returns all groups as a slice sorted by GroupName.
func sortedGroups(m map[string]Group) []Group {
	groups := make([]Group, 0, len(m))
	for _, g := range m {
		groups = append(groups, g)
	}

	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupName < groups[j].GroupName })

	return groups
}

// sortedPolicies returns all policies as a slice sorted by PolicyName.
func sortedPolicies(m map[string]Policy) []Policy {
	policies := make([]Policy, 0, len(m))
	for _, p := range m {
		policies = append(policies, p)
	}

	sort.Slice(policies, func(i, j int) bool { return policies[i].PolicyName < policies[j].PolicyName })

	return policies
}

// sortedInstanceProfiles returns all instance profiles sorted by name.
func sortedInstanceProfiles(m map[string]InstanceProfile) []InstanceProfile {
	ips := make([]InstanceProfile, 0, len(m))
	for _, ip := range m {
		ips = append(ips, ip)
	}

	sort.Slice(ips, func(i, j int) bool { return ips[i].InstanceProfileName < ips[j].InstanceProfileName })

	return ips
}

// UpdateServiceSpecificCredential updates the status of a service-specific credential.
func (b *InMemoryBackend) UpdateServiceSpecificCredential(userName, credentialID, status string) error {
	b.mu.Lock("UpdateServiceSpecificCredential")
	defer b.mu.Unlock()

	if _, exists := b.users[userName]; !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	cred, exists := b.serviceSpecificCreds[credentialID]
	if !exists {
		return fmt.Errorf("%w: service-specific credential %q not found", ErrPolicyNotFound, credentialID)
	}

	if cred.UserName != userName {
		return fmt.Errorf("%w: credential %q does not belong to user %q", ErrPolicyNotFound, credentialID, userName)
	}

	cred.Status = status
	b.serviceSpecificCreds[credentialID] = cred

	return nil
}

// GetAccountSummary returns comprehensive account summary counts.
func (b *InMemoryBackend) GetAccountSummary() AccountSummary {
	b.mu.RLock("GetAccountSummary")
	defer b.mu.RUnlock()

	totalKeys, activeKeys := b.accessKeyCountLocked()
	attachedPolicies := 0

	for _, arns := range b.userPolicies {
		attachedPolicies += len(arns)
	}

	for _, arns := range b.rolePolicies {
		attachedPolicies += len(arns)
	}

	for _, arns := range b.groupPolicies {
		attachedPolicies += len(arns)
	}

	return AccountSummary{
		Users:             len(b.users),
		Groups:            len(b.groups),
		Roles:             len(b.roles),
		Policies:          len(b.policies),
		InstanceProfiles:  len(b.instanceProfiles),
		SAMLProviders:     len(b.samlProviders),
		MFADevices:        len(b.virtualMFADevices),
		AccessKeysPerUser: totalKeys,
		ActiveAccessKeys:  activeKeys,
		AttachedPolicies:  attachedPolicies,
		AccountAliases:    len(b.accountAliases),
		OIDCProviders:     len(b.oidcProviders),
	}
}

// accessKeyCountLocked returns total and active access key counts.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) accessKeyCountLocked() (int, int) {
	var total, active int

	for _, ak := range b.accessKeys {
		total++
		if ak.Status == accessKeyStatusActive {
			active++
		}
	}

	return total, active
}

// credReportConsts holds string literals used in credential report CSV rows.
const (
	credNoInfo  = "no_information"
	credFalse   = "false"
	credTrue    = "true"
	credCsvCols = 22 // 8 fixed + 5 per key × 2 + 4 cert fields
)

// credKeyFields returns the 5 CSV fields for one access key slot (or N/A placeholders).
func credKeyFields(ak *AccessKey) []string {
	if ak == nil {
		return []string{credFalse, notApplicable, notApplicable, notApplicable, notApplicable}
	}

	active := credFalse
	if ak.Status == accessKeyStatusActive {
		active = credTrue
	}

	rotated := ak.CreateDate.UTC().Format(time.RFC3339)

	lastUsedDate := notApplicable
	lastUsedRegion := notApplicable
	lastUsedService := notApplicable

	if ak.LastUsedDate != nil {
		lastUsedDate = ak.LastUsedDate.UTC().Format(time.RFC3339)
		if ak.LastUsedRegion != "" {
			lastUsedRegion = ak.LastUsedRegion
		}

		if ak.LastUsedServiceName != "" {
			lastUsedService = ak.LastUsedServiceName
		}
	}

	return []string{active, rotated, lastUsedDate, lastUsedRegion, lastUsedService}
}

// credUserMFAActive returns "true" if the user has at least one active MFA device.
func credUserMFAActive(userName string, links map[string]string, devices map[string]VirtualMFADevice) string {
	for serial, owner := range links {
		if owner != userName {
			continue
		}

		if dev, ok := devices[serial]; ok && dev.Status == MFAStatusEnabled {
			return credTrue
		}
	}

	return credFalse
}

// GetCredentialReport generates a realistic base64-encoded CSV credential report.
// Each user row reflects actual login-profile and access-key state.
func (b *InMemoryBackend) GetCredentialReport() string {
	b.mu.RLock("GetCredentialReport")
	defer b.mu.RUnlock()

	users := sortedUsers(b.users)
	const extraRows = 2
	lines := make([]string, 0, extraRows+len(users))
	lines = append(lines, credentialReportHeader)

	// Root account row (always present in real AWS).
	rootArn := "arn:aws:iam::" + b.accountID + ":root"
	lines = append(lines, strings.Join([]string{
		"<root_account>", rootArn, time.Now().UTC().Format(time.RFC3339),
		notApplicable, credNoInfo, notApplicable, notApplicable, credFalse,
		credFalse, notApplicable, notApplicable, notApplicable, notApplicable,
		credFalse, notApplicable, notApplicable, notApplicable, notApplicable,
		credFalse, notApplicable, credFalse, notApplicable,
	}, ","))

	c := b.comp()
	c.mu.Lock()
	mfaLinks := maps.Clone(c.mfaUserLinks)
	c.mu.Unlock()

	for _, u := range users {
		lines = append(lines, b.credUserRow(u, mfaLinks))
	}

	return strings.Join(lines, "\n")
}

// credUserRow builds the CSV row for a single IAM user.
func (b *InMemoryBackend) credUserRow(u User, mfaLinks map[string]string) string {
	createdAt := u.CreateDate.UTC().Format(time.RFC3339)

	passwordEnabled := credFalse
	if _, has := b.loginProfiles[u.UserName]; has {
		passwordEnabled = credTrue
	}

	var userKeys []AccessKey
	for _, ak := range b.accessKeys {
		if ak.UserName == u.UserName {
			userKeys = append(userKeys, ak)
		}
	}

	sort.Slice(userKeys, func(i, j int) bool {
		return userKeys[i].CreateDate.Before(userKeys[j].CreateDate)
	})

	keyAt := func(idx int) *AccessKey {
		if idx < len(userKeys) {
			return &userKeys[idx]
		}

		return nil
	}

	mfaActive := credUserMFAActive(u.UserName, mfaLinks, b.virtualMFADevices)

	row := make([]string, 0, credCsvCols)
	row = append(row, u.UserName, u.Arn, createdAt,
		passwordEnabled, credNoInfo, notApplicable, notApplicable,
		mfaActive)
	row = append(row, credKeyFields(keyAt(0))...)
	row = append(row, credKeyFields(keyAt(1))...)
	row = append(row, credFalse, notApplicable, credFalse, notApplicable)

	return strings.Join(row, ",")
}
