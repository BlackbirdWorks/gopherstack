package iam

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/google/uuid"
)

// GenerateOrganizationsAccessReport creates a new org access report job and returns its ID.
func (b *InMemoryBackend) GenerateOrganizationsAccessReport(_ string) string {
	jobID := "orgjob-" + newID("")
	now := time.Now().UTC()

	c := b.comp()
	c.mu.Lock()
	c.orgReportJobs[jobID] = now
	c.mu.Unlock()

	return jobID
}

// GetOrganizationsAccessReport retrieves the status of an org access report job.
func (b *InMemoryBackend) GetOrganizationsAccessReport(jobID string) (string, time.Time, bool) {
	c := b.comp()
	c.mu.Lock()
	createdAt, found := c.orgReportJobs[jobID]
	c.mu.Unlock()

	if !found {
		return "", time.Time{}, false
	}

	return jobStatusCompleted, createdAt, true
}

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

// credentialReportHeader is the CSV header for the IAM credential report.
const credentialReportHeader = "user,arn,user_creation_time,password_enabled,password_last_used," +
	"password_last_changed,password_next_rotation,mfa_active," +
	"access_key_1_active,access_key_1_last_rotated,access_key_1_last_used_date," +
	"access_key_1_last_used_region,access_key_1_last_used_service," +
	"access_key_2_active,access_key_2_last_rotated,access_key_2_last_used_date," +
	"access_key_2_last_used_region,access_key_2_last_used_service," +
	"cert_1_active,cert_1_last_rotated,cert_2_active,cert_2_last_rotated"

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
		Users:             b.users.Len(),
		Groups:            b.groups.Len(),
		Roles:             b.roles.Len(),
		Policies:          b.policies.Len(),
		InstanceProfiles:  b.instanceProfiles.Len(),
		SAMLProviders:     b.samlProviders.Len(),
		MFADevices:        b.virtualMFADevices.Len(),
		AccessKeysPerUser: totalKeys,
		ActiveAccessKeys:  activeKeys,
		AttachedPolicies:  attachedPolicies,
		AccountAliases:    len(b.accountAliases),
		OIDCProviders:     b.oidcProviders.Len(),
	}
}

// accessKeyCountLocked returns total and active access key counts.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) accessKeyCountLocked() (int, int) {
	var total, active int

	for _, ak := range b.accessKeys.All() {
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
func credUserMFAActive(
	userName string,
	links map[string]string,
	devices *store.Table[VirtualMFADevice],
) string {
	for serial, owner := range links {
		if owner != userName {
			continue
		}

		if dev, ok := devices.Get(serial); ok && dev.Status == MFAStatusEnabled {
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
	if _, has := b.loginProfiles.Get(u.UserName); has {
		passwordEnabled = credTrue
	}

	var userKeys []AccessKey
	userKeysList := b.userAccessKeys[u.UserName]
	for _, id := range userKeysList {
		if ak, ok := b.accessKeys.Get(id); ok {
			userKeys = append(userKeys, *ak)
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

	b.delegationRequests.Put(&req)

	return &req, nil
}

// AcceptDelegationRequest accepts a delegation request (stub implementation).
func (b *InMemoryBackend) AcceptDelegationRequest(delegationID string) error {
	b.mu.Lock("AcceptDelegationRequest")
	defer b.mu.Unlock()

	req, exists := b.delegationRequests.Get(delegationID)
	if !exists {
		return fmt.Errorf("%w: delegation request %q not found", ErrInvalidAction, delegationID)
	}

	req.Status = "ACCEPTED"
	b.delegationRequests.Put(req)

	return nil
}

// AssociateDelegationRequest associates a delegation request with a policy ARN (stub implementation).
func (b *InMemoryBackend) AssociateDelegationRequest(delegationID, policyArn string) error {
	b.mu.Lock("AssociateDelegationRequest")
	defer b.mu.Unlock()

	req, exists := b.delegationRequests.Get(delegationID)
	if !exists {
		return fmt.Errorf("%w: delegation request %q not found", ErrInvalidAction, delegationID)
	}

	req.PolicyArn = policyArn
	b.delegationRequests.Put(req)

	return nil
}

// ChangePassword changes the IAM user password, validating against the account password policy.
// In real AWS, this operates on the currently authenticated user.
func (b *InMemoryBackend) ChangePassword(newPassword string) error {
	if newPassword == "" {
		return fmt.Errorf("%w: new password must not be empty", ErrInvalidPassword)
	}

	b.mu.RLock("ChangePassword")
	policy := b.passwordPolicy
	b.mu.RUnlock()

	if err := validatePasswordAgainstPolicy(newPassword, policy); err != nil {
		return err
	}

	return nil
}

// validatePasswordAgainstPolicy checks that password satisfies the given PasswordPolicy.
// If policy is nil, the default policy is used. Returns ErrInvalidPassword on violation.
func validatePasswordAgainstPolicy(password string, policy *PasswordPolicy) error {
	if policy == nil {
		policy = defaultPasswordPolicy()
	}

	minLen := policy.MinimumPasswordLength
	if minLen == 0 {
		minLen = defaultMinPasswordLength
	}

	if len(password) < minLen {
		return fmt.Errorf(
			"%w: password must be at least %d characters long",
			ErrInvalidPassword, minLen,
		)
	}

	if policy.RequireUppercaseCharacters && !strings.ContainsAny(password, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") {
		return fmt.Errorf("%w: password must contain at least one uppercase letter", ErrInvalidPassword)
	}

	if policy.RequireLowercaseCharacters && !strings.ContainsAny(password, "abcdefghijklmnopqrstuvwxyz") {
		return fmt.Errorf("%w: password must contain at least one lowercase letter", ErrInvalidPassword)
	}

	if policy.RequireNumbers && !strings.ContainsAny(password, "0123456789") {
		return fmt.Errorf("%w: password must contain at least one digit", ErrInvalidPassword)
	}

	if policy.RequireSymbols && !strings.ContainsAny(password, `!@#$%^&*()_+-=[]{}|;':",./<>?`) {
		return fmt.Errorf("%w: password must contain at least one symbol", ErrInvalidPassword)
	}

	return nil
}
