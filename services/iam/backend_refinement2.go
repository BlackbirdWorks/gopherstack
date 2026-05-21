package iam

import (
	"fmt"
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

// GetCredentialReport generates a realistic base64-encoded CSV credential report.
// Each user row reflects actual login-profile and access-key state.
func (b *InMemoryBackend) GetCredentialReport() string {
	b.mu.RLock("GetCredentialReport")
	defer b.mu.RUnlock()

	const (
		noInfo   = "no_information"
		falseStr = "false"
		trueStr  = "true"
	)

	users := sortedUsers(b.users)
	const extraRows = 2
	lines := make([]string, 0, extraRows+len(users))
	lines = append(lines, credentialReportHeader)

	// Root account row (always present in real AWS).
	rootArn := "arn:aws:iam::" + b.accountID + ":root"
	lines = append(lines, strings.Join([]string{
		"<root_account>", rootArn, time.Now().UTC().Format(time.RFC3339),
		notApplicable, noInfo, notApplicable, notApplicable, falseStr,
		falseStr, notApplicable, notApplicable, notApplicable, notApplicable,
		falseStr, notApplicable, notApplicable, notApplicable, notApplicable,
		falseStr, notApplicable, falseStr, notApplicable,
	}, ","))

	for _, u := range users {
		createdAt := u.CreateDate.UTC().Format(time.RFC3339)

		// Password / login profile.
		_, hasLoginProfile := b.loginProfiles[u.UserName]
		passwordEnabled := falseStr
		if hasLoginProfile {
			passwordEnabled = trueStr
		}

		// Collect access keys for this user (at most 2 in AWS).
		var userKeys []AccessKey
		for _, ak := range b.accessKeys {
			if ak.UserName == u.UserName {
				userKeys = append(userKeys, ak)
			}
		}

		sort.Slice(userKeys, func(i, j int) bool {
			return userKeys[i].CreateDate.Before(userKeys[j].CreateDate)
		})

		keyFields := func(idx int) []string {
			if idx >= len(userKeys) {
				return []string{falseStr, notApplicable, notApplicable, notApplicable, notApplicable}
			}

			ak := userKeys[idx]
			active := falseStr
			if ak.Status == accessKeyStatusActive {
				active = trueStr
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

		// Determine whether the user has any active MFA device.
		c := b.comp()
		c.mu.Lock()
		mfaActive := falseStr
		for serial, owner := range c.mfaUserLinks {
			if owner == u.UserName {
				if dev, ok := b.virtualMFADevices[serial]; ok && dev.Status == MFAStatusEnabled {
					mfaActive = trueStr
					break
				}
			}
		}
		c.mu.Unlock()

		// 22 CSV columns per row (8 fixed + 5 per key × 2 + 4 cert fields).
		const csvCols = 22
		row := make([]string, 0, csvCols)
		row = append(row, u.UserName, u.Arn, createdAt,
			passwordEnabled, noInfo, notApplicable, notApplicable,
			mfaActive)
		row = append(row, keyFields(0)...)
		row = append(row, keyFields(1)...)
		row = append(row, falseStr, notApplicable, falseStr, notApplicable)
		lines = append(lines, strings.Join(row, ","))
	}

	return strings.Join(lines, "\n")
}
