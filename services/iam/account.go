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

// ---- Outbound Web Identity Federation ----
//
// Real AWS's EnableOutboundWebIdentityFederation/
// DisableOutboundWebIdentityFederation/GetOutboundWebIdentityFederationInfo
// (aws-sdk-go-v2/service/iam@v1.55.0's api_op_*.go) gate whether IAM
// principals in the account may call STS's GetWebIdentityToken -- see
// services/sts/web_identity.go's OutboundWebIdentityFederationEnabled check,
// wired via the optional-capability type assertion in
// services/sts/store.go's SetOIDCLookup (no cli.go changes needed: cli.go
// already passes the full IAM backend to STS via SetOIDCLookup for OIDC
// provider lookups, and that same object now also satisfies STS's
// AccountSettingsLookup interface).

// outboundFederationIssuerURL deterministically derives the "unique issuer
// URL" GetOutboundWebIdentityFederationInfo/EnableOutboundWebIdentityFederation
// return for accountID. Real AWS does not publish the algorithm it uses to
// generate this URL, so this is a synthetic-but-plausible placeholder (URL to
// a fixed host that would host to the documented
// /.well-known/openid-configuration and /.well-known/jwks.json discovery
// endpoints per the real Output field's doc comment) rather than a claimed
// match to AWS's real value -- computed on demand from accountID instead of
// stored, so there is nothing extra to persist/restore.
func outboundFederationIssuerURL(accountID string) string {
	return "https://oidc-federation.gopherstack.local/" + accountID
}

// EnableOutboundWebIdentityFederation turns on outbound web identity
// federation for this account (see the "Outbound Web Identity Federation"
// section above) and returns the account's issuer URL.
func (b *InMemoryBackend) EnableOutboundWebIdentityFederation() string {
	b.mu.Lock("EnableOutboundWebIdentityFederation")
	defer b.mu.Unlock()

	b.outboundFederationEnabled = true

	return outboundFederationIssuerURL(b.accountID)
}

// DisableOutboundWebIdentityFederation turns off outbound web identity
// federation for this account. Per the real API's doc comment, this does not
// retroactively invalidate JWTs already issued by GetWebIdentityToken -- this
// emulator does not track issued-JWT identity after the fact either way, so
// there is nothing additional to revoke.
func (b *InMemoryBackend) DisableOutboundWebIdentityFederation() {
	b.mu.Lock("DisableOutboundWebIdentityFederation")
	defer b.mu.Unlock()

	b.outboundFederationEnabled = false
}

// GetOutboundWebIdentityFederationInfo returns the account's issuer URL and
// current enabled/disabled status.
func (b *InMemoryBackend) GetOutboundWebIdentityFederationInfo() (string, bool) {
	b.mu.RLock("GetOutboundWebIdentityFederationInfo")
	defer b.mu.RUnlock()

	return outboundFederationIssuerURL(b.accountID), b.outboundFederationEnabled
}

// OutboundWebIdentityFederationEnabled reports whether outbound web identity
// federation is currently enabled for this account. This satisfies
// services/sts's AccountSettingsLookup optional-capability interface (see the
// package doc comment above), letting STS's GetWebIdentityToken gate on this
// setting without any sts<->iam interface living in cli.go.
func (b *InMemoryBackend) OutboundWebIdentityFederationEnabled() bool {
	b.mu.RLock("OutboundWebIdentityFederationEnabled")
	defer b.mu.RUnlock()

	return b.outboundFederationEnabled
}

// GenerateOrganizationsAccessReport creates a new org access report job and returns its ID.
func (b *InMemoryBackend) GenerateOrganizationsAccessReport(_ string) string {
	jobID := "orgjob-" + newID("")
	now := time.Now().UTC()

	b.mu.Lock("GenerateOrganizationsAccessReport")
	defer b.mu.Unlock()

	b.comp().orgReportJobs[jobID] = now

	return jobID
}

// GetOrganizationsAccessReport retrieves the status of an org access report job.
func (b *InMemoryBackend) GetOrganizationsAccessReport(jobID string) (string, time.Time, bool) {
	b.mu.RLock("GetOrganizationsAccessReport")
	defer b.mu.RUnlock()

	createdAt, found := b.comp().orgReportJobs[jobID]
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
		Users:                      b.users.Len(),
		Groups:                     b.groups.Len(),
		Roles:                      b.roles.Len(),
		Policies:                   b.policies.Len(),
		InstanceProfiles:           b.instanceProfiles.Len(),
		SAMLProviders:              b.samlProviders.Len(),
		MFADevices:                 b.virtualMFADevices.Len(),
		AccessKeysPerUser:          totalKeys,
		ActiveAccessKeys:           activeKeys,
		AttachedPolicies:           attachedPolicies,
		AccountAliases:             len(b.accountAliases),
		OIDCProviders:              b.oidcProviders.Len(),
		GlobalEndpointTokenVersion: globalEndpointTokenVersionOrdinal(b.globalEndpointTokenVersion),
	}
}

// globalEndpointTokenVersionOrdinal maps the stored GlobalEndpointTokenVersion
// enum to GetAccountSummary's SummaryMap integer value, per the SDK's
// SummaryKeyTypeGlobalEndpointTokenVersion entry (aws-sdk-go-v2/service/iam,
// types/enums.go). Real IAM exposes this preference only through that summary
// map -- SetSecurityTokenServicePreferences itself has no dedicated getter.
// globalEndpointTokenVersionOrdinalV1/V2 are GetAccountSummary's
// GlobalEndpointTokenVersion SummaryMap integer values for v1Token/v2Token.
const (
	globalEndpointTokenVersionOrdinalV1 = 1
	globalEndpointTokenVersionOrdinalV2 = 2
)

func globalEndpointTokenVersionOrdinal(version string) int {
	if version == globalEndpointTokenVersionV2 {
		return globalEndpointTokenVersionOrdinalV2
	}

	return globalEndpointTokenVersionOrdinalV1
}

// SetSecurityTokenServicePreferences sets the account's global endpoint token
// version, observable afterward via GetAccountSummary's
// GlobalEndpointTokenVersion entry (SetSecurityTokenServicePreferences itself
// returns no body).
func (b *InMemoryBackend) SetSecurityTokenServicePreferences(globalEndpointTokenVersion string) error {
	if globalEndpointTokenVersion != globalEndpointTokenVersionV1 &&
		globalEndpointTokenVersion != globalEndpointTokenVersionV2 {
		return fmt.Errorf("%w: GlobalEndpointTokenVersion must be v1Token or v2Token", ErrValidationError)
	}

	b.mu.Lock("SetSecurityTokenServicePreferences")
	defer b.mu.Unlock()

	b.globalEndpointTokenVersion = globalEndpointTokenVersion

	return nil
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

	mfaLinks := maps.Clone(b.comp().mfaUserLinks)

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

// delegationRequestArnResource is the resource-type segment gopherstack uses
// for delegation request ARNs (arn:aws:iam::<account>:delegation-request/<id>).
// Real AWS does not document an ARN format for delegation requests, but
// GetHumanReadableSummary's EntityArn ("At this time, the only supported
// entity type is delegation-request") requires one to resolve requests by
// ARN, so this is a synthetic-but-plausible placeholder, not a claimed match.
const delegationRequestArnResource = "delegation-request/"

// delegationRequestConsoleDeepLink deterministically derives
// CreateDelegationRequestOutput.ConsoleDeepLink. Real AWS does not publish
// this URL's format, so this is a synthetic-but-plausible placeholder
// (mirrors outboundFederationIssuerURL above), computed on demand rather
// than stored.
func delegationRequestConsoleDeepLink(delegationID string) string {
	return "https://console.gopherstack.local/iam/delegation-requests/" + delegationID
}

// delegationRequestIDFromArn extracts the delegation request ID from an
// EntityArn built by delegationRequestArnResource, e.g.
// "arn:aws:iam::123456789012:delegation-request/abc" -> "abc".
func delegationRequestIDFromArn(entityArn string) (string, bool) {
	idx := strings.LastIndex(entityArn, delegationRequestArnResource)
	if idx == -1 {
		return "", false
	}

	id := entityArn[idx+len(delegationRequestArnResource):]
	if id == "" {
		return "", false
	}

	return id, true
}

// CreateDelegationRequest creates a delegation request. Caller (the handler)
// has already validated in's required members.
func (b *InMemoryBackend) CreateDelegationRequest(in CreateDelegationRequestInput) (*DelegationRequest, error) {
	delegationID := uuid.New().String()

	b.mu.Lock("CreateDelegationRequest")
	defer b.mu.Unlock()

	req := DelegationRequest{
		DelegationID:         delegationID,
		TargetAccountID:      in.OwnerAccountID,
		Status:               "PENDING",
		CreateDate:           time.Now().UTC(),
		Description:          in.Description,
		NotificationChannel:  in.NotificationChannel,
		RequestorWorkflowID:  in.RequestorWorkflowID,
		SessionDuration:      in.SessionDuration,
		OnlySendByOwner:      in.OnlySendByOwner,
		RedirectURL:          in.RedirectURL,
		RequestMessage:       in.RequestMessage,
		PolicyTemplateArn:    in.PolicyTemplateArn,
		PermissionParameters: in.PermissionParameters,
	}

	b.delegationRequests.Put(&req)

	return &req, nil
}

// DelegationRequestExists reports whether a delegation request with the
// given ID exists. Used by GetHumanReadableSummary to distinguish a real
// (but unsummarizable, see PARITY.md) entity from an unknown one.
func (b *InMemoryBackend) DelegationRequestExists(delegationID string) bool {
	b.mu.RLock("DelegationRequestExists")
	defer b.mu.RUnlock()

	_, exists := b.delegationRequests.Get(delegationID)

	return exists
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

// ChangePassword changes the IAM user password, validating OldPassword against the
// account's current password and NewPassword against the account password policy.
// In real AWS, this operates on the currently authenticated user; this mock tracks a
// single account-wide current password since it has no per-request caller identity.
func (b *InMemoryBackend) ChangePassword(oldPassword, newPassword string) error {
	if oldPassword == "" {
		return fmt.Errorf("%w: OldPassword must not be empty", ErrOldPasswordIncorrect)
	}

	if newPassword == "" {
		return fmt.Errorf("%w: new password must not be empty", ErrInvalidPassword)
	}

	b.mu.Lock("ChangePassword")
	defer b.mu.Unlock()

	if b.currentPassword != "" && oldPassword != b.currentPassword {
		return fmt.Errorf("%w: old password does not match", ErrOldPasswordIncorrect)
	}

	if err := validatePasswordAgainstPolicy(newPassword, b.passwordPolicy); err != nil {
		return err
	}

	b.currentPassword = newPassword

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
