package iam_test

// AWS-accuracy audit batch-1 — IAM ID format, access-key limits, policy-version
// lifecycle, password-policy enforcement, and credential report fidelity.

import (
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// ---- Access Key ID format ----

func TestAccessKeyID_Format(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateUser("alice", "/", "")
	require.NoError(t, err)

	ak, err := b.CreateAccessKey("alice")
	require.NoError(t, err)

	// AWS access key IDs are exactly 20 characters starting with "AKIA".
	assert.Equal(t, 20, len(ak.AccessKeyID), "access key ID must be 20 chars")
	assert.True(t, strings.HasPrefix(ak.AccessKeyID, "AKIA"), "access key ID must start with AKIA")

	// All characters after prefix must be uppercase alphanumeric (A–Z, 0–9).
	for _, ch := range ak.AccessKeyID {
		assert.True(t, unicode.IsUpper(ch) || unicode.IsDigit(ch),
			"access key ID char %q must be uppercase alphanumeric", ch)
	}
}

func TestAccessKeyID_NoDashes(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateUser("bob", "/", "")
	require.NoError(t, err)

	ak, err := b.CreateAccessKey("bob")
	require.NoError(t, err)

	assert.NotContains(t, ak.AccessKeyID, "-", "access key ID must not contain dashes")
	assert.NotContains(t, ak.AccessKeyID, strings.ToLower(ak.AccessKeyID[4:]),
		"access key ID suffix must not be all lowercase")
}

func TestAccessKeyID_Unique(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateUser("carol", "/", "")
	require.NoError(t, err)

	ak1, err := b.CreateAccessKey("carol")
	require.NoError(t, err)

	// Need to delete first key before creating second (max 2, but test uniqueness).
	require.NoError(t, b.DeleteAccessKey("carol", ak1.AccessKeyID))

	ak2, err := b.CreateAccessKey("carol")
	require.NoError(t, err)

	assert.NotEqual(t, ak1.AccessKeyID, ak2.AccessKeyID)
}

// ---- Entity ID format (AIDA, AROA, AGPA, ANPA, AIPA) ----

func TestUserID_Format(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	u, err := b.CreateUser("alice", "/", "")
	require.NoError(t, err)

	// User IDs are AIDA + 16 uppercase alphanumeric chars.
	assert.True(t, strings.HasPrefix(u.UserID, "AIDA"), "user ID must start with AIDA")
	assert.Equal(t, 20, len(u.UserID), "user ID must be 20 chars")
	assert.NotContains(t, u.UserID, "-", "user ID must not contain dashes")

	for _, ch := range u.UserID {
		assert.True(t, unicode.IsUpper(ch) || unicode.IsDigit(ch),
			"user ID char %q must be uppercase alphanumeric", ch)
	}
}

func TestRoleID_Format(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	r, err := b.CreateRole("MyRole", "/", `{"Version":"2012-10-17","Statement":[]}`, "")
	require.NoError(t, err)

	// Role IDs are AROA + 16 uppercase alphanumeric chars.
	assert.True(t, strings.HasPrefix(r.RoleID, "AROA"), "role ID must start with AROA")
	assert.Equal(t, 20, len(r.RoleID), "role ID must be 20 chars")
	assert.NotContains(t, r.RoleID, "-", "role ID must not contain dashes")

	for _, ch := range r.RoleID {
		assert.True(t, unicode.IsUpper(ch) || unicode.IsDigit(ch),
			"role ID char %q must be uppercase alphanumeric", ch)
	}
}

func TestGroupID_Format(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	g, err := b.CreateGroup("Admins", "/")
	require.NoError(t, err)

	// Group IDs are AGPA + 16 uppercase alphanumeric chars.
	assert.True(t, strings.HasPrefix(g.GroupID, "AGPA"), "group ID must start with AGPA")
	assert.Equal(t, 20, len(g.GroupID), "group ID must be 20 chars")
	assert.NotContains(t, g.GroupID, "-", "group ID must not contain dashes")

	for _, ch := range g.GroupID {
		assert.True(t, unicode.IsUpper(ch) || unicode.IsDigit(ch),
			"group ID char %q must be uppercase alphanumeric", ch)
	}
}

func TestPolicyID_Format(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	p, err := b.CreatePolicy("MyPolicy", "/", `{"Version":"2012-10-17","Statement":[]}`)
	require.NoError(t, err)

	// Policy IDs are ANPA + 16 uppercase alphanumeric chars.
	assert.True(t, strings.HasPrefix(p.PolicyID, "ANPA"), "policy ID must start with ANPA")
	assert.Equal(t, 20, len(p.PolicyID), "policy ID must be 20 chars")
	assert.NotContains(t, p.PolicyID, "-", "policy ID must not contain dashes")

	for _, ch := range p.PolicyID {
		assert.True(t, unicode.IsUpper(ch) || unicode.IsDigit(ch),
			"policy ID char %q must be uppercase alphanumeric", ch)
	}
}

func TestInstanceProfileID_Format(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	ip, err := b.CreateInstanceProfile("MyProfile", "/")
	require.NoError(t, err)

	// Instance profile IDs are AIPA + 16 uppercase alphanumeric chars.
	assert.True(t, strings.HasPrefix(ip.InstanceProfileID, "AIPA"),
		"instance profile ID must start with AIPA")
	assert.Equal(t, 20, len(ip.InstanceProfileID), "instance profile ID must be 20 chars")
	assert.NotContains(t, ip.InstanceProfileID, "-", "instance profile ID must not contain dashes")

	for _, ch := range ip.InstanceProfileID {
		assert.True(t, unicode.IsUpper(ch) || unicode.IsDigit(ch),
			"instance profile ID char %q must be uppercase alphanumeric", ch)
	}
}

// ---- Max 2 access keys per user (AWS limit) ----

func TestCreateAccessKey_MaxTwoPerUser(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateUser("dave", "/", "")
	require.NoError(t, err)

	ak1, err := b.CreateAccessKey("dave")
	require.NoError(t, err)
	assert.NotEmpty(t, ak1.AccessKeyID)

	ak2, err := b.CreateAccessKey("dave")
	require.NoError(t, err)
	assert.NotEmpty(t, ak2.AccessKeyID)

	// Third key must fail.
	_, err = b.CreateAccessKey("dave")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrLimitExceeded,
		"third access key must return LimitExceeded")
}

func TestCreateAccessKey_LimitPerUser_IndependentAcrossUsers(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")
	_, _ = b.CreateUser("bob", "/", "")

	// Fill alice's quota.
	_, err := b.CreateAccessKey("alice")
	require.NoError(t, err)
	_, err = b.CreateAccessKey("alice")
	require.NoError(t, err)

	// Bob should still be able to create keys.
	_, err = b.CreateAccessKey("bob")
	require.NoError(t, err, "alice's limit must not affect bob")
}

// ---- PolicyVersion lifecycle ----

func TestCreatePolicyVersion_LimitExceededError(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	p, err := b.CreatePolicy("P", "/", doc)
	require.NoError(t, err)

	// v1 is implicit; create v2–v5 to fill the 5-version limit.
	for range 4 {
		_, err = b.CreatePolicyVersion(p.Arn, doc, false)
		require.NoError(t, err)
	}

	// 6th version must fail with LimitExceeded (not DeleteConflict).
	_, err = b.CreatePolicyVersion(p.Arn, doc, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrLimitExceeded,
		"exceeding 5 policy versions must return LimitExceeded")
	assert.NotErrorIs(t, err, iam.ErrDeleteConflict,
		"policy version limit must NOT return DeleteConflict")
}

func TestCreatePolicyVersion_MonotonicVersionID(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	p, err := b.CreatePolicy("Mono", "/", doc)
	require.NoError(t, err)

	v2, err := b.CreatePolicyVersion(p.Arn, doc, false)
	require.NoError(t, err)
	assert.Equal(t, "v2", v2.VersionID)

	v3, err := b.CreatePolicyVersion(p.Arn, doc, false)
	require.NoError(t, err)
	assert.Equal(t, "v3", v3.VersionID)

	// Delete v2; next version must be v4 (monotonic), not v3 (collision).
	require.NoError(t, b.DeletePolicyVersion(p.Arn, "v2"))

	v4, err := b.CreatePolicyVersion(p.Arn, doc, false)
	require.NoError(t, err)
	assert.Equal(t, "v4", v4.VersionID,
		"version ID must be monotonically increasing even after deletes")
}

func TestCreatePolicyVersion_V1CountsTowardLimit(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	p, err := b.CreatePolicy("LimitTest", "/", doc)
	require.NoError(t, err)

	// v1 is implicit; only 4 more should succeed.
	for i := range 4 {
		_, err = b.CreatePolicyVersion(p.Arn, doc, false)
		require.NoError(t, err, "version %d should succeed", i+2)
	}

	// 5th new version (6th total) must fail.
	_, err = b.CreatePolicyVersion(p.Arn, doc, false)
	require.ErrorIs(t, err, iam.ErrLimitExceeded)
}

func TestSetDefaultPolicyVersion_UpdatesDefault(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc1 := `{"Version":"2012-10-17","Statement":[]}`
	doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}`

	p, err := b.CreatePolicy("DefaultTest", "/", doc1)
	require.NoError(t, err)

	v2, err := b.CreatePolicyVersion(p.Arn, doc2, false)
	require.NoError(t, err)

	require.NoError(t, b.SetDefaultPolicyVersion(p.Arn, v2.VersionID))

	versions, err := b.ListPolicyVersions(p.Arn)
	require.NoError(t, err)

	var defaultCount int
	for _, v := range versions {
		if v.IsDefaultVersion {
			defaultCount++
			assert.Equal(t, "v2", v.VersionID, "v2 must be the new default")
		}
	}

	assert.Equal(t, 1, defaultCount, "exactly one version must be default")
}

func TestDeletePolicyVersion_CannotDeleteDefault(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	p, err := b.CreatePolicy("DelDefault", "/", doc)
	require.NoError(t, err)

	// v1 is the default; deleting it must fail.
	err = b.DeletePolicyVersion(p.Arn, "v1")
	require.Error(t, err, "deleting the default version must fail")
}

// ---- PasswordPolicy enforcement ----

func TestPasswordPolicy_ChangePassword_MinLength(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Set policy requiring 12-char minimum.
	require.NoError(t, b.UpdateAccountPasswordPolicy(iam.PasswordPolicy{
		MinimumPasswordLength: 12,
	}))

	err := b.ChangePassword("short")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrInvalidPassword,
		"password below minimum length must return ErrInvalidPassword")
}

func TestPasswordPolicy_ChangePassword_Uppercase(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	require.NoError(t, b.UpdateAccountPasswordPolicy(iam.PasswordPolicy{
		MinimumPasswordLength:      8,
		RequireUppercaseCharacters: true,
	}))

	err := b.ChangePassword("alllower1")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrInvalidPassword, "no uppercase must fail")

	err = b.ChangePassword("HasUpper1")
	require.NoError(t, err, "password with uppercase must succeed")
}

func TestPasswordPolicy_ChangePassword_Lowercase(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	require.NoError(t, b.UpdateAccountPasswordPolicy(iam.PasswordPolicy{
		MinimumPasswordLength:      8,
		RequireLowercaseCharacters: true,
	}))

	err := b.ChangePassword("ALLUPPER1")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrInvalidPassword, "no lowercase must fail")

	err = b.ChangePassword("haslower1")
	require.NoError(t, err, "password with lowercase must succeed")
}

func TestPasswordPolicy_ChangePassword_Numbers(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	require.NoError(t, b.UpdateAccountPasswordPolicy(iam.PasswordPolicy{
		MinimumPasswordLength: 8,
		RequireNumbers:        true,
	}))

	err := b.ChangePassword("NoDigits!")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrInvalidPassword, "no digit must fail")

	err = b.ChangePassword("HasDigit1")
	require.NoError(t, err, "password with digit must succeed")
}

func TestPasswordPolicy_ChangePassword_Symbols(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	require.NoError(t, b.UpdateAccountPasswordPolicy(iam.PasswordPolicy{
		MinimumPasswordLength: 8,
		RequireSymbols:        true,
	}))

	err := b.ChangePassword("NoSymbol1")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrInvalidPassword, "no symbol must fail")

	err = b.ChangePassword("HasSymbl!")
	require.NoError(t, err, "password with symbol must succeed")
}

func TestPasswordPolicy_CreateLoginProfile_MinLength(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")

	require.NoError(t, b.UpdateAccountPasswordPolicy(iam.PasswordPolicy{
		MinimumPasswordLength: 10,
	}))

	_, err := b.CreateLoginProfile("alice", "short", false)
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrInvalidPassword)

	_, err = b.CreateLoginProfile("alice", "longenough1", false)
	require.NoError(t, err)
}

func TestPasswordPolicy_UpdateLoginProfile_Enforced(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")
	_, err := b.CreateLoginProfile("alice", "InitialPass1!", false)
	require.NoError(t, err)

	require.NoError(t, b.UpdateAccountPasswordPolicy(iam.PasswordPolicy{
		MinimumPasswordLength:      12,
		RequireUppercaseCharacters: true,
		RequireNumbers:             true,
	}))

	// Password below minimum.
	err = b.UpdateLoginProfile("alice", "Short1A", false)
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrInvalidPassword)

	// Password missing uppercase.
	err = b.UpdateLoginProfile("alice", "alllower123", false)
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrInvalidPassword)

	// Valid password.
	err = b.UpdateLoginProfile("alice", "GoodPassword123", false)
	require.NoError(t, err)
}

func TestPasswordPolicy_DefaultAllowsShortPassword(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")

	// Default policy: min 8 chars, no complexity requirements.
	_, err := b.CreateLoginProfile("alice", "exactly8!", false)
	require.NoError(t, err, "8-char password must satisfy default policy")
}

// ---- CredentialReport fidelity ----

func TestCredentialReport_AccessKeyLastUsed_Reflected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")

	ak, err := b.CreateAccessKey("alice")
	require.NoError(t, err)

	// Record access key usage.
	b.RecordAccessKeyUsage(ak.AccessKeyID, "us-east-1", "s3")

	report := b.GetCredentialReport()

	// The access key last used date should be present in the report.
	lines := strings.Split(report, "\n")
	var aliceRow string
	for _, line := range lines {
		if strings.HasPrefix(line, "alice,") {
			aliceRow = line
		}
	}

	require.NotEmpty(t, aliceRow, "alice must appear in credential report")
	assert.NotContains(t, aliceRow, "N/A,N/A,s3",
		"access key last used date must not be N/A after usage is recorded")
	assert.Contains(t, aliceRow, "s3",
		"service name from access key usage must appear in report")
	assert.Contains(t, aliceRow, "us-east-1",
		"region from access key usage must appear in report")
}

func TestCredentialReport_MFAActive_Reflected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")

	report := b.GetCredentialReport()
	lines := strings.Split(report, "\n")

	var aliceRow string
	for _, line := range lines {
		if strings.HasPrefix(line, "alice,") {
			aliceRow = line
		}
	}

	require.NotEmpty(t, aliceRow)
	fields := strings.Split(aliceRow, ",")

	// mfa_active is the 8th field (index 7, 0-based).
	require.GreaterOrEqual(t, len(fields), 8)
	assert.Equal(t, "false", fields[7], "mfa_active must be false before MFA is enabled")

	// Enable MFA device.
	dev, err := b.CreateVirtualMFADeviceFull("alice-mfa", "/mfa/")
	require.NoError(t, err)
	require.NoError(t, b.EnableMFADevice("alice", dev.SerialNumber, "123456", "789012"))

	report2 := b.GetCredentialReport()
	lines2 := strings.Split(report2, "\n")

	var aliceRow2 string
	for _, line := range lines2 {
		if strings.HasPrefix(line, "alice,") {
			aliceRow2 = line
		}
	}

	require.NotEmpty(t, aliceRow2)
	fields2 := strings.Split(aliceRow2, ",")
	require.GreaterOrEqual(t, len(fields2), 8)
	assert.Equal(t, "true", fields2[7], "mfa_active must be true after MFA device is enabled")
}

func TestCredentialReport_PasswordEnabled_Reflected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")

	report := b.GetCredentialReport()
	lines := strings.Split(report, "\n")
	var aliceRow string
	for _, line := range lines {
		if strings.HasPrefix(line, "alice,") {
			aliceRow = line
		}
	}

	require.NotEmpty(t, aliceRow)
	fields := strings.Split(aliceRow, ",")

	// password_enabled is field index 3.
	require.GreaterOrEqual(t, len(fields), 4)
	assert.Equal(t, "false", fields[3], "password_enabled must be false before login profile created")

	_, err := b.CreateLoginProfile("alice", "Password123!", false)
	require.NoError(t, err)

	report2 := b.GetCredentialReport()
	lines2 := strings.Split(report2, "\n")
	for _, line := range lines2 {
		if strings.HasPrefix(line, "alice,") {
			aliceRow = line
		}
	}

	fields2 := strings.Split(aliceRow, ",")
	require.GreaterOrEqual(t, len(fields2), 4)
	assert.Equal(t, "true", fields2[3], "password_enabled must be true after login profile created")
}

func TestCredentialReport_RootRowPresent(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	report := b.GetCredentialReport()
	assert.Contains(t, report, "<root_account>", "credential report must include root account row")
}

func TestCredentialReport_HasHeader(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	report := b.GetCredentialReport()
	lines := strings.Split(report, "\n")
	require.GreaterOrEqual(t, len(lines), 1)
	assert.True(t, strings.HasPrefix(lines[0], "user,arn,"),
		"first line must be CSV header starting with user,arn,")
}

// ---- ARN format accuracy ----

func TestUserARN_Format(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackendWithConfig("123456789012")
	u, err := b.CreateUser("alice", "/", "")
	require.NoError(t, err)

	assert.Equal(t, "arn:aws:iam::123456789012:user/alice", u.Arn)
}

func TestUserARN_WithPath(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackendWithConfig("123456789012")
	u, err := b.CreateUser("alice", "/division/team/", "")
	require.NoError(t, err)

	assert.Equal(t, "arn:aws:iam::123456789012:user/division/team/alice", u.Arn)
}

func TestRoleARN_Format(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackendWithConfig("123456789012")
	r, err := b.CreateRole("MyRole", "/", `{"Version":"2012-10-17","Statement":[]}`, "")
	require.NoError(t, err)

	assert.Equal(t, "arn:aws:iam::123456789012:role/MyRole", r.Arn)
}

func TestGroupARN_Format(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackendWithConfig("123456789012")
	g, err := b.CreateGroup("Admins", "/")
	require.NoError(t, err)

	assert.Equal(t, "arn:aws:iam::123456789012:group/Admins", g.Arn)
}

func TestPolicyARN_Format(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackendWithConfig("123456789012")
	p, err := b.CreatePolicy("MyPolicy", "/", `{"Version":"2012-10-17","Statement":[]}`)
	require.NoError(t, err)

	assert.Equal(t, "arn:aws:iam::123456789012:policy/MyPolicy", p.Arn)
}

// ---- AccountSummary accuracy ----

func TestGetAccountSummary_Users(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("u1", "/", "")
	_, _ = b.CreateUser("u2", "/", "")

	sum := b.GetAccountSummary()
	assert.Equal(t, 2, sum.Users)
}

func TestGetAccountSummary_AccessKeys(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")
	ak1, _ := b.CreateAccessKey("alice")
	_, _ = b.CreateAccessKey("alice")

	// Deactivate one key.
	_ = b.UpdateAccessKey("alice", ak1.AccessKeyID, "Inactive")

	sum := b.GetAccountSummary()
	assert.Equal(t, 2, sum.AccessKeysPerUser, "total access keys")
	assert.Equal(t, 1, sum.ActiveAccessKeys, "only active keys")
}

func TestGetAccountSummary_Policies(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreatePolicy("P1", "/", doc)
	_, _ = b.CreatePolicy("P2", "/", doc)

	sum := b.GetAccountSummary()
	assert.Equal(t, 2, sum.Policies)
}

// ---- UpdateAccessKey status ----

func TestUpdateAccessKey_ActivateDeactivate(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")
	ak, err := b.CreateAccessKey("alice")
	require.NoError(t, err)
	assert.Equal(t, "Active", ak.Status)

	require.NoError(t, b.UpdateAccessKey("alice", ak.AccessKeyID, "Inactive"))
	keys, err := b.ListAccessKeys("alice", "", 10)
	require.NoError(t, err)
	require.Len(t, keys.Data, 1)
	assert.Equal(t, "Inactive", keys.Data[0].Status)

	require.NoError(t, b.UpdateAccessKey("alice", ak.AccessKeyID, "Active"))
	keys2, err := b.ListAccessKeys("alice", "", 10)
	require.NoError(t, err)
	assert.Equal(t, "Active", keys2.Data[0].Status)
}

func TestUpdateAccessKey_NotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")

	err := b.UpdateAccessKey("alice", "AKIANONEXISTENT12345", "Inactive")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrAccessKeyNotFound)
}

// ---- GetAccessKeyLastUsed ----

func TestGetAccessKeyLastUsed_NotUsed(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")
	ak, _ := b.CreateAccessKey("alice")

	result, err := b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	assert.Equal(t, "N/A", result.LastUsedDate, "new key must report N/A for last used date")
}

func TestGetAccessKeyLastUsed_AfterUsage(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")
	ak, _ := b.CreateAccessKey("alice")

	b.RecordAccessKeyUsage(ak.AccessKeyID, "us-west-2", "ec2")

	result, err := b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	assert.NotEqual(t, "N/A", result.LastUsedDate, "last used date must be set after usage")
	assert.Equal(t, "us-west-2", result.Region)
	assert.Equal(t, "ec2", result.ServiceName)
}

// ---- PasswordPolicy defaults ----

func TestGetAccountPasswordPolicy_Defaults(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	pp := b.GetAccountPasswordPolicy()

	assert.Equal(t, 8, pp.MinimumPasswordLength, "default min password length is 8")
	assert.False(t, pp.RequireUppercaseCharacters)
	assert.False(t, pp.RequireLowercaseCharacters)
	assert.False(t, pp.RequireNumbers)
	assert.False(t, pp.RequireSymbols)
	assert.True(t, pp.AllowUsersToChangePassword)
}

func TestDeleteAccountPasswordPolicy_ResetsToDefault(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	require.NoError(t, b.UpdateAccountPasswordPolicy(iam.PasswordPolicy{
		MinimumPasswordLength:      16,
		RequireUppercaseCharacters: true,
	}))

	require.NoError(t, b.DeleteAccountPasswordPolicy())

	pp := b.GetAccountPasswordPolicy()
	assert.Equal(t, 8, pp.MinimumPasswordLength)
	assert.False(t, pp.RequireUppercaseCharacters)
}

// ---- DeletePolicy enforcement ----

func TestDeletePolicy_FailsWhenAttachedToUser(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateUser("alice", "/", "")
	p, err := b.CreatePolicy("P", "/", doc)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("alice", p.Arn))

	err = b.DeletePolicy(p.Arn)
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrDeleteConflict)
}

func TestDeletePolicy_FailsWhenAttachedToRole(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateRole("R", "/", doc, "")
	p, err := b.CreatePolicy("P", "/", doc)
	require.NoError(t, err)
	require.NoError(t, b.AttachRolePolicy("R", p.Arn))

	err = b.DeletePolicy(p.Arn)
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrDeleteConflict)
}

func TestDeletePolicy_SucceedsWhenDetached(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateUser("alice", "/", "")
	p, err := b.CreatePolicy("P", "/", doc)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("alice", p.Arn))
	require.NoError(t, b.DetachUserPolicy("alice", p.Arn))
	require.NoError(t, b.DeletePolicy(p.Arn))
}

// ---- DeleteUser enforcement ----

func TestDeleteUser_FailsWhenAttachedPoliciesExist(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateUser("alice", "/", "")
	p, _ := b.CreatePolicy("P", "/", doc)
	require.NoError(t, b.AttachUserPolicy("alice", p.Arn))

	err := b.DeleteUser("alice")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrDeleteConflict)
}

func TestDeleteUser_FailsWhenInlinePoliciesExist(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")
	doc := `{"Version":"2012-10-17","Statement":[]}`
	require.NoError(t, b.PutUserPolicy("alice", "MyPolicy", doc))

	err := b.DeleteUser("alice")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrDeleteConflict)
}

// ---- PolicyVersion snapshot/restore ----

func TestCreatePolicyVersion_MonotonicAfterRestore(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	p, err := b.CreatePolicy("SnapTest", "/", doc)
	require.NoError(t, err)

	v2, err := b.CreatePolicyVersion(p.Arn, doc, false)
	require.NoError(t, err)
	assert.Equal(t, "v2", v2.VersionID)

	// Snapshot and restore.
	snap := b.Snapshot()
	b2 := iam.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	// Next version must be v3, not v2.
	v3, err := b2.CreatePolicyVersion(p.Arn, doc, false)
	require.NoError(t, err)
	assert.Equal(t, "v3", v3.VersionID,
		"after restore, version counter must continue from where it left off")
}

// ---- Instance profile operations ----

func TestAddRoleToInstanceProfile_OnlyOneRole(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateRole("R1", "/", doc, "")
	_, _ = b.CreateRole("R2", "/", doc, "")
	_, _ = b.CreateInstanceProfile("MyProfile", "/")

	require.NoError(t, b.AddRoleToInstanceProfile("MyProfile", "R1"))

	// Adding a second role must fail (AWS allows only one role per instance profile).
	err := b.AddRoleToInstanceProfile("MyProfile", "R2")
	require.Error(t, err, "adding second role to instance profile must fail")
}

func TestInstanceProfile_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")
	ip, err := b.CreateInstanceProfile("MyProfile", "/")
	require.NoError(t, err)

	require.NoError(t, b.AddRoleToInstanceProfile("MyProfile", "MyRole"))

	got, err := b.GetInstanceProfile("MyProfile")
	require.NoError(t, err)
	assert.Equal(t, ip.InstanceProfileName, got.InstanceProfileName)
	assert.Len(t, got.Roles, 1)
	assert.Equal(t, "MyRole", got.Roles[0])
}

// ---- SAMLProvider operations ----

func TestSAMLProvider_CRUD(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := "<md>test</md>"

	sp, err := b.CreateSAMLProvider("MySAML", doc)
	require.NoError(t, err)
	assert.Contains(t, sp.Arn, "saml-provider/MySAML")

	got, err := b.GetSAMLProvider(sp.Arn)
	require.NoError(t, err)
	assert.Equal(t, doc, got.SAMLMetadataDocument)

	updated, err := b.UpdateSAMLProvider(sp.Arn, "<md>updated</md>")
	require.NoError(t, err)
	assert.Equal(t, "<md>updated</md>", updated.SAMLMetadataDocument)

	all, err := b.ListSAMLProviders()
	require.NoError(t, err)
	assert.Len(t, all, 1)

	require.NoError(t, b.DeleteSAMLProvider(sp.Arn))

	_, err = b.GetSAMLProvider(sp.Arn)
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrSAMLProviderNotFound)
}

// ---- OIDCProvider operations ----

func TestOIDCProvider_CRUD(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	clientIDs := []string{"client1"}
	thumbprints := []string{"abc123"}

	p, err := b.CreateOpenIDConnectProvider("https://example.com", clientIDs, thumbprints)
	require.NoError(t, err)
	assert.Contains(t, p.Arn, "oidc-provider")

	got, err := b.GetOpenIDConnectProvider(p.Arn)
	require.NoError(t, err)
	assert.Equal(t, clientIDs, got.ClientIDList)

	require.NoError(t, b.AddClientIDToOpenIDConnectProvider(p.Arn, "client2"))
	got2, _ := b.GetOpenIDConnectProvider(p.Arn)
	assert.Contains(t, got2.ClientIDList, "client2")

	require.NoError(t, b.RemoveClientIDFromOpenIDConnectProvider(p.Arn, "client1"))
	got3, _ := b.GetOpenIDConnectProvider(p.Arn)
	assert.NotContains(t, got3.ClientIDList, "client1")

	all, err := b.ListOpenIDConnectProviders()
	require.NoError(t, err)
	assert.Len(t, all, 1)

	require.NoError(t, b.DeleteOpenIDConnectProvider(p.Arn))

	_, err = b.GetOpenIDConnectProvider(p.Arn)
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrOIDCProviderNotFound)
}

// ---- MFA device operations ----

func TestVirtualMFADevice_CRUD(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")

	dev, err := b.CreateVirtualMFADeviceFull("alice-token", "/mfa/")
	require.NoError(t, err)
	assert.NotEmpty(t, dev.SerialNumber)
	assert.NotEmpty(t, dev.Base32StringSeed)

	require.NoError(t, b.EnableMFADevice("alice", dev.SerialNumber, "123456", "789012"))

	devices, err := b.ListMFADevicesForUser("alice")
	require.NoError(t, err)
	assert.Len(t, devices, 1)

	require.NoError(t, b.DeactivateMFADevice("alice", dev.SerialNumber))

	devices2, err := b.ListMFADevicesForUser("alice")
	require.NoError(t, err)
	assert.Empty(t, devices2)

	require.NoError(t, b.DeleteVirtualMFADevice(dev.SerialNumber))
}

// ---- Server certificate operations ----

func TestServerCertificate_AccuracyAudit(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	const certBody = "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----"

	sc, err := b.UploadServerCertificate("MyCert", "/", certBody, "")
	require.NoError(t, err)
	assert.Equal(t, "MyCert", sc.ServerCertificateName)

	got, err := b.GetServerCertificate("MyCert")
	require.NoError(t, err)
	assert.Equal(t, certBody, got.CertificateBody)

	certs, err := b.ListServerCertificates("/")
	require.NoError(t, err)
	assert.Len(t, certs, 1)

	require.NoError(t, b.UpdateServerCertificate("MyCert", "NewName", "/new/"))

	_, err = b.GetServerCertificate("MyCert")
	require.Error(t, err, "old name must not exist after rename")

	got2, err := b.GetServerCertificate("NewName")
	require.NoError(t, err)
	assert.Equal(t, "NewName", got2.ServerCertificateName)

	require.NoError(t, b.DeleteServerCertificate("NewName"))
	_, err = b.GetServerCertificate("NewName")
	require.Error(t, err)
}

// ---- PermissionsBoundary operations ----

func TestPermissionsBoundary_UserRoundTrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateUser("alice", "/", "")
	p, _ := b.CreatePolicy("Boundary", "/", doc)

	require.NoError(t, b.PutUserPermissionsBoundary("alice", p.Arn))
	u, _ := b.GetUser("alice")
	assert.Equal(t, p.Arn, u.PermissionsBoundary)

	require.NoError(t, b.DeleteUserPermissionsBoundary("alice"))
	u2, _ := b.GetUser("alice")
	assert.Empty(t, u2.PermissionsBoundary)
}

func TestPermissionsBoundary_RoleRoundTrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")
	p, _ := b.CreatePolicy("Boundary", "/", doc)

	require.NoError(t, b.PutRolePermissionsBoundary("MyRole", p.Arn))
	r, _ := b.GetRole("MyRole")
	assert.Equal(t, p.Arn, r.PermissionsBoundary)

	require.NoError(t, b.DeleteRolePermissionsBoundary("MyRole"))
	r2, _ := b.GetRole("MyRole")
	assert.Empty(t, r2.PermissionsBoundary)
}

// ---- UpdateAssumeRolePolicy ----

func TestUpdateAssumeRolePolicy_Valid(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")

	newDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	require.NoError(t, b.UpdateAssumeRolePolicy("MyRole", newDoc))

	r, err := b.GetRole("MyRole")
	require.NoError(t, err)
	assert.Equal(t, newDoc, r.AssumeRolePolicyDocument)
}

// ---- AccountAlias ----

func TestAccountAlias_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	require.NoError(t, b.CreateAccountAlias("my-account"))
	aliases := b.ListAccountAliases()
	assert.Equal(t, []string{"my-account"}, aliases)

	// AWS allows only one alias; creating another replaces the old one.
	require.NoError(t, b.CreateAccountAlias("new-alias"))
	aliases2 := b.ListAccountAliases()
	assert.Equal(t, []string{"new-alias"}, aliases2)

	require.NoError(t, b.DeleteAccountAlias("new-alias"))
	aliases3 := b.ListAccountAliases()
	assert.Empty(t, aliases3)
}

// ---- ListEntitiesForPolicy ----

func TestListEntitiesForPolicy_AllEntityTypes(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateUser("alice", "/", "")
	_, _ = b.CreateRole("R", "/", doc, "")
	_, _ = b.CreateGroup("G", "/")
	p, _ := b.CreatePolicy("P", "/", doc)

	require.NoError(t, b.AttachUserPolicy("alice", p.Arn))
	require.NoError(t, b.AttachRolePolicy("R", p.Arn))
	require.NoError(t, b.AttachGroupPolicy("G", p.Arn))

	entities, err := b.ListEntitiesForPolicy(p.Arn, "")
	require.NoError(t, err)
	assert.Len(t, entities.PolicyUsers, 1)
	assert.Len(t, entities.PolicyRoles, 1)
	assert.Len(t, entities.PolicyGroups, 1)
}

func TestListEntitiesForPolicy_FilterByType(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateUser("alice", "/", "")
	_, _ = b.CreateRole("R", "/", doc, "")
	p, _ := b.CreatePolicy("P", "/", doc)
	require.NoError(t, b.AttachUserPolicy("alice", p.Arn))
	require.NoError(t, b.AttachRolePolicy("R", p.Arn))

	users, err := b.ListEntitiesForPolicy(p.Arn, "User")
	require.NoError(t, err)
	assert.Len(t, users.PolicyUsers, 1)
	assert.Empty(t, users.PolicyRoles)

	roles, err := b.ListEntitiesForPolicy(p.Arn, "Role")
	require.NoError(t, err)
	assert.Len(t, roles.PolicyRoles, 1)
	assert.Empty(t, roles.PolicyUsers)
}

// ---- UpdateUser / UpdateRole / UpdateGroup ----

func TestUpdateUser_RenameAndPath(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")

	require.NoError(t, b.UpdateUser("alice", "/new/", "alicia"))

	_, err := b.GetUser("alice")
	require.Error(t, err, "old name must not exist after rename")

	u, err := b.GetUser("alicia")
	require.NoError(t, err)
	assert.Equal(t, "/new/", u.Path)
}

func TestUpdateGroup_Rename(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateGroup("OldGroup", "/")

	require.NoError(t, b.UpdateGroup("OldGroup", "/", "NewGroup"))

	_, err := b.GetGroup("OldGroup")
	require.Error(t, err, "old name must not exist after rename")

	g, err := b.GetGroup("NewGroup")
	require.NoError(t, err)
	assert.Equal(t, "NewGroup", g.GroupName)
}
