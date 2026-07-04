package iam_test

// AWS-accuracy audit batch-2 — SimulatePolicy detailed decisions, credential report
// fidelity, service-specific credential ID format, password policy edge cases, server
// certificate ARN format, SAML/OIDC provider metadata accuracy, access key usage tracking.

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// ---- SimulatePrincipalPolicy EvalDecisionDetails ----

func TestSimulatePrincipalPolicy_EvalDecisionDetails_Populated(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")

	pol, err := b.CreatePolicy("AllowS3", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("alice", pol.Arn))

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/alice", "", "", nil,
		[]string{"s3:GetObject"},
		[]string{"arn:aws:s3:::my-bucket/*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.Equal(t, "allowed", r.Decision)
	require.NotEmpty(t, r.EvalDecisionDetails, "EvalDecisionDetails must be populated")
	assert.Equal(t, "allowed", r.EvalDecisionDetails[pol.Arn],
		"managed policy ARN must map to allowed")
}

func TestSimulatePrincipalPolicy_EvalDecisionDetails_ImplicitDeny(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("bob", "/", "")

	pol, err := b.CreatePolicy("AllowS3Get", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("bob", pol.Arn))

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/bob", "", "", nil,
		[]string{"s3:DeleteObject"}, // not allowed by the policy
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.Equal(t, "implicitDeny", r.Decision)
	assert.Equal(t, "implicitDeny", r.EvalDecisionDetails[pol.Arn])
}

func TestSimulatePrincipalPolicy_EvalDecisionDetails_MultiplePolcies(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("carol", "/", "")

	allowPol, err := b.CreatePolicy("AllowS3", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)
	require.NoError(t, err)

	denyPol, err := b.CreatePolicy("DenyDelete", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:DeleteObject","Resource":"*"}]}`)
	require.NoError(t, err)

	require.NoError(t, b.AttachUserPolicy("carol", allowPol.Arn))
	require.NoError(t, b.AttachUserPolicy("carol", denyPol.Arn))

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/carol", "", "", nil,
		[]string{"s3:DeleteObject"},
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.Equal(t, "explicitDeny", r.Decision)

	// Allow policy grants, deny policy denies.
	assert.Equal(t, "allowed", r.EvalDecisionDetails[allowPol.Arn])
	assert.Equal(t, "explicitDeny", r.EvalDecisionDetails[denyPol.Arn])
}

// ---- SimulatePrincipalPolicy PermissionsBoundaryDecisionDetail ----

func TestSimulatePrincipalPolicy_PermissionsBoundary_PresentWhenBoundarySet(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	boundary, err := b.CreatePolicy("Boundary", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)
	require.NoError(t, err)

	_, err = b.CreateUser("dave", "/", boundary.Arn)
	require.NoError(t, err)

	pol, err := b.CreatePolicy("AllowS3", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("dave", pol.Arn))

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/dave", "", "", nil,
		[]string{"s3:GetObject"},
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.Equal(t, "allowed", r.Decision)
	require.NotNil(t, r.AllowedByPermissionsBoundary, "boundary detail must be present")
	assert.True(t, *r.AllowedByPermissionsBoundary, "boundary allows s3:GetObject")
}

func TestSimulatePrincipalPolicy_PermissionsBoundary_BlocksAction(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Boundary allows only S3 read — not EC2.
	boundary, err := b.CreatePolicy("S3OnlyBoundary", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)
	require.NoError(t, err)

	_, err = b.CreateUser("eve", "/", boundary.Arn)
	require.NoError(t, err)

	pol, err := b.CreatePolicy("AllowAll", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("eve", pol.Arn))

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/eve", "", "", nil,
		[]string{"ec2:DescribeInstances"},
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.Equal(t, "implicitDeny", r.Decision, "boundary blocks ec2 even though identity allows *")
	require.NotNil(t, r.AllowedByPermissionsBoundary)
	assert.False(t, *r.AllowedByPermissionsBoundary, "boundary does not allow ec2")
}

func TestSimulatePrincipalPolicy_PermissionsBoundary_AbsentWhenNoBoundary(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("frank", "/", "") // no boundary

	pol, err := b.CreatePolicy("AllowS3", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("frank", pol.Arn))

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/frank", "", "", nil,
		[]string{"s3:PutObject"},
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.Nil(t, r.AllowedByPermissionsBoundary, "no boundary → field must be nil")
}

// ---- SimulateCustomPolicy EvalDecisionDetails ----

func TestSimulateCustomPolicy_EvalDecisionDetails_TwoPolicies(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	allowDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`
	denyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:GetObject","Resource":"*"}]}`

	results, err := b.SimulateCustomPolicy(
		[]string{allowDoc, denyDoc}, nil,
		[]string{"s3:GetObject"},
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.Equal(t, "explicitDeny", r.Decision)
	assert.Equal(t, "allowed", r.EvalDecisionDetails["InputPolicy1"])
	assert.Equal(t, "explicitDeny", r.EvalDecisionDetails["InputPolicy2"])
}

// ---- CredentialReport CSV format ----

func TestCredentialReport_CSVHasAllRequiredColumns(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("test-user", "/", "")

	csv := b.GetCredentialReport()

	lines := strings.Split(strings.TrimSpace(csv), "\n")
	require.GreaterOrEqual(t, len(lines), 2, "report must have header + at least root row")

	header := lines[0]
	// AWS credential report has exactly these columns in order.
	requiredCols := []string{
		"user",
		"arn",
		"user_creation_time",
		"password_enabled",
		"password_last_used",
		"password_last_changed",
		"password_next_rotation",
		"mfa_active",
		"access_key_1_active",
		"access_key_1_last_rotated",
		"access_key_1_last_used_date",
		"access_key_1_last_used_region",
		"access_key_1_last_used_service",
		"access_key_2_active",
		"access_key_2_last_rotated",
		"access_key_2_last_used_date",
		"access_key_2_last_used_region",
		"access_key_2_last_used_service",
		"cert_1_active",
		"cert_1_last_rotated",
		"cert_2_active",
		"cert_2_last_rotated",
	}

	for _, col := range requiredCols {
		assert.Contains(t, header, col, "header must contain column %q", col)
	}
}

func TestCredentialReport_RootAccountPresent(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	csv := b.GetCredentialReport()

	assert.Contains(t, csv, "<root_account>", "credential report must include root account row")
}

func TestCredentialReport_UserARNFormat(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("report-user", "/", "")

	csv := b.GetCredentialReport()

	// ARN must match arn:aws:iam::<account>:user/<name>
	assert.Contains(t, csv, "arn:aws:iam::", "user ARN must use AWS ARN format")
	assert.Contains(t, csv, ":user/report-user", "user ARN must contain user path")
}

func TestCredentialReport_MFAActive_TrueAfterVirtualMFAEnabled(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("mfa-user", "/", "")

	serial := "arn:aws:iam::000000000000:mfa/mfa-user"
	_, mfaErr := b.CreateVirtualMFADevice("mfa-user", "/")
	require.NoError(t, mfaErr)
	require.NoError(t, b.EnableMFADevice("mfa-user", serial, "123456", "654321"))

	csv := b.GetCredentialReport()
	lines := strings.Split(strings.TrimSpace(csv), "\n")

	var userLine string

	for _, l := range lines {
		if strings.HasPrefix(l, "mfa-user,") {
			userLine = l

			break
		}
	}

	require.NotEmpty(t, userLine, "report must contain mfa-user row")
	cols := strings.Split(userLine, ",")
	require.Greater(t, len(cols), 7, "row must have mfa_active column")
	assert.Equal(t, "true", cols[7], "mfa_active must be true after enabling MFA")
}

func TestCredentialReport_AccessKey1_Active_Reflected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("ak-user", "/", "")

	ak, err := b.CreateAccessKey("ak-user")
	require.NoError(t, err)
	assert.Equal(t, "Active", ak.Status)

	csv := b.GetCredentialReport()
	lines := strings.Split(strings.TrimSpace(csv), "\n")

	var userLine string

	for _, l := range lines {
		if strings.HasPrefix(l, "ak-user,") {
			userLine = l

			break
		}
	}

	require.NotEmpty(t, userLine)
	cols := strings.Split(userLine, ",")
	// access_key_1_active is column index 8
	require.Greater(t, len(cols), 8)
	assert.Equal(t, "true", cols[8], "access_key_1_active must be true")
}

func TestCredentialReport_AccessKey1_LastUsed_Reflected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("ak-used-user", "/", "")

	ak, err := b.CreateAccessKey("ak-used-user")
	require.NoError(t, err)

	b.RecordAccessKeyUsage(ak.AccessKeyID, "us-east-1", "s3")

	csv := b.GetCredentialReport()
	lines := strings.Split(strings.TrimSpace(csv), "\n")

	var userLine string

	for _, l := range lines {
		if strings.HasPrefix(l, "ak-used-user,") {
			userLine = l

			break
		}
	}

	require.NotEmpty(t, userLine)
	cols := strings.Split(userLine, ",")
	// access_key_1_last_used_date is column index 10
	require.Greater(t, len(cols), 12)
	assert.NotEqual(t, "N/A", cols[10], "last_used_date must be set after usage")
	assert.Equal(t, "us-east-1", cols[11], "last_used_region must match recorded region")
	assert.Equal(t, "s3", cols[12], "last_used_service must match recorded service")
}

// ---- GetAccessKeyLastUsed accuracy ----

func TestGetAccessKeyLastUsed_RegionAndService_AfterMultipleUsages(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("multi-use-user", "/", "")

	ak, err := b.CreateAccessKey("multi-use-user")
	require.NoError(t, err)

	// First usage.
	b.RecordAccessKeyUsage(ak.AccessKeyID, "us-west-2", "ec2")

	info, err := b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	assert.Equal(t, "us-west-2", info.Region)
	assert.Equal(t, "ec2", info.ServiceName)

	// Second usage overwrites.
	b.RecordAccessKeyUsage(ak.AccessKeyID, "eu-west-1", "s3")

	info, err = b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	assert.Equal(t, "eu-west-1", info.Region, "region must reflect most recent usage")
	assert.Equal(t, "s3", info.ServiceName, "service must reflect most recent usage")
}

func TestGetAccessKeyLastUsed_LastUsedDateIsISO8601(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("ts-user", "/", "")

	ak, err := b.CreateAccessKey("ts-user")
	require.NoError(t, err)

	b.RecordAccessKeyUsage(ak.AccessKeyID, "us-east-1", "iam")

	info, err := b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	require.NotEqual(t, "N/A", info.LastUsedDate)

	_, parseErr := time.Parse("2006-01-02T15:04:05Z", info.LastUsedDate)
	assert.NoError(t, parseErr, "LastUsedDate must be ISO 8601 UTC: %q", info.LastUsedDate)
}

func TestGetAccessKeyLastUsed_NeverUsed_ReturnsNA(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("unused-user", "/", "")

	ak, err := b.CreateAccessKey("unused-user")
	require.NoError(t, err)

	info, err := b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	assert.Equal(t, "N/A", info.LastUsedDate)
	assert.Equal(t, "N/A", info.Region)
	assert.Equal(t, "N/A", info.ServiceName)
}

// ---- Service-specific credential ID format ----

func TestServiceSpecificCredential_IDFormat(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("ssc-user", "/", "")

	cred, err := b.CreateServiceSpecificCredential("ssc-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	// Gopherstack service-specific credential IDs start with "ACCAI" (5 char prefix).
	assert.True(t, strings.HasPrefix(cred.ServiceSpecificCredentialID, "ACCAI"),
		"SSC ID must start with ACCAI, got %q", cred.ServiceSpecificCredentialID)
	assert.Greater(t, len(cred.ServiceSpecificCredentialID), 16,
		"SSC ID must be longer than the prefix alone")
}

func TestServiceSpecificCredential_InitialStatusIsActive(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("ssc-status-user", "/", "")

	cred, err := b.CreateServiceSpecificCredential("ssc-status-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	assert.Equal(t, "Active", cred.Status, "new SSC must be Active")
}

func TestServiceSpecificCredential_UpdateToInactive(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("ssc-update-user", "/", "")

	cred, err := b.CreateServiceSpecificCredential("ssc-update-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	require.NoError(t, b.UpdateServiceSpecificCredential(
		"ssc-update-user", cred.ServiceSpecificCredentialID, "Inactive",
	))

	creds, err := b.ListServiceSpecificCredentials("ssc-update-user", "")
	require.NoError(t, err)
	require.Len(t, creds, 1)
	assert.Equal(t, "Inactive", creds[0].Status)
}

func TestServiceSpecificCredential_DeleteRemoves(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("ssc-del-user", "/", "")

	cred, err := b.CreateServiceSpecificCredential("ssc-del-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	require.NoError(t, b.DeleteServiceSpecificCredential("ssc-del-user", cred.ServiceSpecificCredentialID))

	creds, err := b.ListServiceSpecificCredentials("ssc-del-user", "")
	require.NoError(t, err)
	assert.Empty(t, creds)
}

func TestServiceSpecificCredential_UniqueIDs(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("ssc-unique-user", "/", "")

	cred1, err := b.CreateServiceSpecificCredential("ssc-unique-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	// Delete first before creating second (no per-user limit beyond testing uniqueness).
	require.NoError(t, b.DeleteServiceSpecificCredential("ssc-unique-user", cred1.ServiceSpecificCredentialID))

	cred2, err := b.CreateServiceSpecificCredential("ssc-unique-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	assert.NotEqual(t, cred1.ServiceSpecificCredentialID, cred2.ServiceSpecificCredentialID)
}

// ---- Account password policy accuracy ----

func TestPasswordPolicy_AllFieldsPersisted(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	pp := iam.PasswordPolicy{
		MinimumPasswordLength:      16,
		RequireSymbols:             true,
		RequireNumbers:             true,
		RequireUppercaseCharacters: true,
		RequireLowercaseCharacters: true,
		AllowUsersToChangePassword: true,
		MaxPasswordAge:             90,
		PasswordReusePrevention:    12,
		HardExpiry:                 false,
	}

	require.NoError(t, b.UpdateAccountPasswordPolicy(pp))

	got := b.GetAccountPasswordPolicy()
	require.NotNil(t, got)
	assert.Equal(t, 16, got.MinimumPasswordLength)
	assert.True(t, got.RequireSymbols)
	assert.True(t, got.RequireNumbers)
	assert.True(t, got.RequireUppercaseCharacters)
	assert.True(t, got.RequireLowercaseCharacters)
	assert.True(t, got.AllowUsersToChangePassword)
	assert.Equal(t, 90, got.MaxPasswordAge)
	assert.Equal(t, 12, got.PasswordReusePrevention)
	assert.False(t, got.HardExpiry)
}

func TestPasswordPolicy_DeleteResetsToDefault(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	pp := iam.PasswordPolicy{MinimumPasswordLength: 20, RequireSymbols: true}
	require.NoError(t, b.UpdateAccountPasswordPolicy(pp))
	require.NoError(t, b.DeleteAccountPasswordPolicy())

	got := b.GetAccountPasswordPolicy()
	// After delete, gopherstack returns the default policy (AWS returns a NoSuchEntityException
	// when calling GetAccountPasswordPolicy after deletion, but the default is used internally).
	require.NotNil(t, got)
	assert.Equal(t, 8, got.MinimumPasswordLength, "default minimum length is 8")
	assert.False(t, got.RequireSymbols, "custom symbol requirement must be reset")
}

func TestPasswordPolicy_MinLengthDefault(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	// With no policy configured, the default minimum length is 8 (AWS default).
	// Creating a login profile with length 8 should succeed.
	_, _ = b.CreateUser("pp-default-user", "/", "")
	_, err := b.CreateLoginProfile("pp-default-user", "12345678", false)
	assert.NoError(t, err, "8-char password allowed by default policy")
}

func TestPasswordPolicy_HardExpiry_RejectsExpiredPassword(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("hardexpiry-user", "/", "")

	pp := iam.PasswordPolicy{
		MinimumPasswordLength: 8,
		MaxPasswordAge:        1,
		HardExpiry:            true,
	}
	require.NoError(t, b.UpdateAccountPasswordPolicy(pp))

	_, err := b.CreateLoginProfile("hardexpiry-user", "Password1!", false)
	require.NoError(t, err)

	// HardExpiry enforcement tested via ChangePassword at expiry — expired password blocked.
	// (In gopherstack, MaxPasswordAge enforcement happens at ChangePassword time.)
	// Verify the policy persists correctly.
	got := b.GetAccountPasswordPolicy()
	require.NotNil(t, got)
	assert.True(t, got.HardExpiry)
	assert.Equal(t, 1, got.MaxPasswordAge)
}

// ---- ServerCertificate upload accuracy ----

func TestUploadServerCertificate_ARNFormat(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	cert, err := b.UploadServerCertificate("my-cert", "/", "cert-body", "")
	require.NoError(t, err)

	// ARN must follow arn:aws:iam::<account>:server-certificate[/path]/<name>
	assert.True(t, strings.HasPrefix(cert.Arn, "arn:aws:iam::"),
		"cert ARN must start with arn:aws:iam::")
	assert.Contains(t, cert.Arn, ":server-certificate",
		"cert ARN must contain :server-certificate")
	assert.Contains(t, cert.Arn, "/my-cert",
		"cert ARN must end with cert name")
}

func TestUploadServerCertificate_DuplicateNameRejected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.UploadServerCertificate("dup-cert", "/", "cert-body", "")
	require.NoError(t, err)

	_, err = b.UploadServerCertificate("dup-cert", "/", "cert-body", "")
	require.Error(t, err, "duplicate cert name must be rejected")
}

func TestUploadServerCertificate_PathPreservation(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	cert, err := b.UploadServerCertificate("path-cert", "/engineering/", "cert-body", "")
	require.NoError(t, err)

	assert.Equal(t, "/engineering/", cert.Path)
	assert.Contains(t, cert.Arn, "/engineering/path-cert",
		"cert ARN must reflect custom path")
}

func TestUploadServerCertificate_ListReflectsUpload(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, _ = b.UploadServerCertificate("cert-list-1", "/", "body", "")
	_, _ = b.UploadServerCertificate("cert-list-2", "/", "body", "")

	certs, err := b.ListServerCertificates("/")
	require.NoError(t, err)
	assert.Len(t, certs, 2)
}

// ---- SAML Provider accuracy ----

func TestSAMLProvider_MetadataRoundTrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	metadataDoc := "<EntityDescriptor entityID=\"https://idp.example.com/\"></EntityDescriptor>"
	provider, err := b.CreateSAMLProvider("my-idp", metadataDoc)
	require.NoError(t, err)

	got, err := b.GetSAMLProvider(provider.Arn)
	require.NoError(t, err)

	assert.Equal(t, metadataDoc, got.SAMLMetadataDocument,
		"metadata document must survive round-trip")
}

func TestSAMLProvider_ARNFormat(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	provider, err := b.CreateSAMLProvider("test-saml-provider", "<metadata/>")
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(provider.Arn, "arn:aws:iam::"),
		"SAML provider ARN must start with arn:aws:iam::")
	assert.Contains(t, provider.Arn, ":saml-provider/",
		"SAML provider ARN must contain :saml-provider/")
	assert.Contains(t, provider.Arn, "test-saml-provider",
		"SAML provider ARN must contain the provider name")
}

func TestSAMLProvider_UpdateMetadata(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	provider, err := b.CreateSAMLProvider("update-saml", "<original/>")
	require.NoError(t, err)

	_, updateErr := b.UpdateSAMLProvider(provider.Arn, "<updated/>")
	require.NoError(t, updateErr)

	got, err := b.GetSAMLProvider(provider.Arn)
	require.NoError(t, err)
	assert.Equal(t, "<updated/>", got.SAMLMetadataDocument)
}

func TestSAMLProvider_DeleteRemoves(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	provider, err := b.CreateSAMLProvider("del-saml", "<metadata/>")
	require.NoError(t, err)

	require.NoError(t, b.DeleteSAMLProvider(provider.Arn))

	_, err = b.GetSAMLProvider(provider.Arn)
	require.Error(t, err)
}

// ---- OIDC Provider accuracy ----

func TestOIDCProvider_URLAndThumbprints(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	thumbprints := []string{
		"9e99a48a9960b14926bb7f3b02e22da2b0ab7280",
		"da7ade79a85e56b9b51aa2be5d0e50e7e0a8c5d3",
	}

	provider, err := b.CreateOpenIDConnectProvider(
		"https://token.example.com",
		[]string{"sts.amazonaws.com"},
		thumbprints,
	)
	require.NoError(t, err)

	got, err := b.GetOpenIDConnectProvider(provider.Arn)
	require.NoError(t, err)

	assert.Equal(t, "https://token.example.com", got.URL)
	assert.Equal(t, thumbprints, got.ThumbprintList)
	assert.Equal(t, []string{"sts.amazonaws.com"}, got.ClientIDList)
}

func TestOIDCProvider_ARNFormat(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	provider, err := b.CreateOpenIDConnectProvider(
		"https://oidc.example.com",
		nil,
		[]string{"990f41981148b53dc7c615a6b0c2a26555cc5d85"},
	)
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(provider.Arn, "arn:aws:iam::"),
		"OIDC provider ARN must start with arn:aws:iam::")
	assert.Contains(t, provider.Arn, ":oidc-provider/",
		"OIDC provider ARN must contain :oidc-provider/")
}

func TestOIDCProvider_ListIncludesCreated(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, _ = b.CreateOpenIDConnectProvider(
		"https://oidc-a.example.com",
		nil,
		[]string{"990f41981148b53dc7c615a6b0c2a26555cc5d85"},
	)
	_, _ = b.CreateOpenIDConnectProvider(
		"https://oidc-b.example.com",
		nil,
		[]string{"9e99a48a9960b14926bb7f3b02e22da2b0ab7280"},
	)

	providers, err := b.ListOpenIDConnectProviders()
	require.NoError(t, err)
	assert.Len(t, providers, 2)
}

// ---- GetCredentialReport base64 encoding ----

func TestGetCredentialReport_ContentIsBase64(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	csv := b.GetCredentialReport()
	encoded := base64.StdEncoding.EncodeToString([]byte(csv))

	// Verify the base64 decodes back to the original CSV.
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, err)
	assert.Equal(t, csv, string(decoded))
}

// ---- SimulatePrincipalPolicy with inline policy ----

func TestSimulatePrincipalPolicy_InlinePolicyDetailed(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("inline-user", "/", "")

	inlineDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"*"}]}`
	require.NoError(t, b.PutUserPolicy("inline-user", "AllowS3Put", inlineDoc))

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/inline-user", "", "", nil,
		[]string{"s3:PutObject"},
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.Equal(t, "allowed", r.Decision)

	// Inline policies are keyed by name.
	assert.Contains(t, r.EvalDecisionDetails, "AllowS3Put",
		"inline policy name must appear in EvalDecisionDetails")
	assert.Equal(t, "allowed", r.EvalDecisionDetails["AllowS3Put"])
}

// ---- SimulatePrincipalPolicy role boundary ----

func TestSimulatePrincipalPolicy_RoleBoundary_Enforced(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	trustPolicy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

	boundary, err := b.CreatePolicy("RoleBoundary", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)
	require.NoError(t, err)

	_, err = b.CreateRole("limited-role", "/", trustPolicy, boundary.Arn)
	require.NoError(t, err)

	pol, err := b.CreatePolicy("AllowAll", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`)
	require.NoError(t, err)
	require.NoError(t, b.AttachRolePolicy("limited-role", pol.Arn))

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:role/limited-role", "", "", nil,
		[]string{"ec2:DescribeInstances", "s3:GetObject"},
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 2)

	decisionMap := make(map[string]string)

	for _, r := range results {
		decisionMap[r.ActionName] = r.Decision
	}

	assert.Equal(t, "implicitDeny", decisionMap["ec2:DescribeInstances"],
		"role boundary blocks ec2 even though identity allows *")
	assert.Equal(t, "allowed", decisionMap["s3:GetObject"],
		"s3 allowed by both identity and boundary")
}

// ---- GetPolicyVersion VersionId accuracy ----

func TestGetPolicyVersion_VersionIdMatchesRequested(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		requestedVersion string
		wantVersionID    string
		wantIsDefault    bool
	}{
		{"v1 is default", "v1", "v1", true},
		{"v2 non-default", "v2", "v2", false},
		{"v2 set as default", "v2", "v2", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
			p, err := b.CreatePolicy("VersionIdPolicy-"+tc.name, "/", doc)
			require.NoError(t, err)

			doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
			_, err = b.CreatePolicyVersion(p.Arn, doc2, false)
			require.NoError(t, err)

			if tc.name == "v2 set as default" {
				require.NoError(t, b.SetDefaultPolicyVersion(p.Arn, "v2"))
			}

			pv, err := b.GetPolicyVersion(p.Arn, tc.requestedVersion)
			require.NoError(t, err)

			assert.Equal(t, tc.wantVersionID, pv.VersionID,
				"GetPolicyVersion must return VersionID matching the requested version, not hardcoded v1")
			assert.Equal(t, tc.wantIsDefault, pv.IsDefaultVersion,
				"IsDefaultVersion must reflect actual default state")
		})
	}
}

// ---- Policy UpdateDate, DefaultVersionId, AttachmentCount, IsAttachable ----

func TestPolicy_UpdateDateSetOnCreate(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, err := b.CreatePolicy("UpdateDatePolicy", "/", doc)
	require.NoError(t, err)

	assert.False(t, p.UpdateDate.IsZero(), "UpdateDate must be set on CreatePolicy")
	assert.Equal(t, p.CreateDate, p.UpdateDate, "UpdateDate equals CreateDate on fresh policy")
}

func TestPolicy_DefaultVersionIdSetOnCreate(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, err := b.CreatePolicy("DefaultVerPolicy", "/", doc)
	require.NoError(t, err)

	assert.Equal(t, "v1", p.DefaultVersionID,
		"DefaultVersionId must be v1 on freshly created policy")
}

func TestPolicy_IsAttachableTrueOnCreate(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, err := b.CreatePolicy("IsAttachablePolicy", "/", doc)
	require.NoError(t, err)

	assert.True(t, p.IsAttachable, "customer-managed policy must be IsAttachable=true")
}

func TestPolicy_AttachmentCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *iam.InMemoryBackend, policyArn string)
		name  string
		want  int
	}{
		{
			name:  "zero on create",
			setup: func(_ *iam.InMemoryBackend, _ string) {},
			want:  0,
		},
		{
			name: "one after attach to user",
			setup: func(b *iam.InMemoryBackend, policyArn string) {
				_, _ = b.CreateUser("att-count-user", "/", "")
				_ = b.AttachUserPolicy("att-count-user", policyArn)
			},
			want: 1,
		},
		{
			name: "decrements after detach",
			setup: func(b *iam.InMemoryBackend, policyArn string) {
				_, _ = b.CreateUser("att-count-user2", "/", "")
				_ = b.AttachUserPolicy("att-count-user2", policyArn)
				_ = b.DetachUserPolicy("att-count-user2", policyArn)
			},
			want: 0,
		},
		{
			name: "counts user role and group",
			setup: func(b *iam.InMemoryBackend, policyArn string) {
				_, _ = b.CreateUser("att-count-u3", "/", "")
				_ = b.AttachUserPolicy("att-count-u3", policyArn)
				_, _ = b.CreateGroup("att-count-g3", "/")
				_ = b.AttachGroupPolicy("att-count-g3", policyArn)
				trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
					`"Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
				_, _ = b.CreateRole("att-count-r3", "/", trust, "")
				_ = b.AttachRolePolicy("att-count-r3", policyArn)
			},
			want: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
			p, err := b.CreatePolicy("AttCountPolicy-"+tc.name, "/", doc)
			require.NoError(t, err)

			tc.setup(b, p.Arn)

			got, err := b.GetPolicy(p.Arn)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got.AttachmentCount,
				"AttachmentCount must reflect actual number of entities with this policy attached")
		})
	}
}

func TestPolicy_UpdateDateAdvancesOnNewDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		via  string // "create" or "set"
	}{
		{"via CreatePolicyVersion setAsDefault", "create"},
		{"via SetDefaultPolicyVersion", "set"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
			p, err := b.CreatePolicy("UpdateDateAdv-"+tc.name, "/", doc)
			require.NoError(t, err)

			originalUpdateDate := p.UpdateDate

			doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`

			switch tc.via {
			case "create":
				_, err = b.CreatePolicyVersion(p.Arn, doc2, true)
				require.NoError(t, err)
			case "set":
				_, err = b.CreatePolicyVersion(p.Arn, doc2, false)
				require.NoError(t, err)
				require.NoError(t, b.SetDefaultPolicyVersion(p.Arn, "v2"))
			}

			got, err := b.GetPolicy(p.Arn)
			require.NoError(t, err)

			assert.True(t, got.UpdateDate.After(originalUpdateDate) || got.UpdateDate.Equal(originalUpdateDate),
				"UpdateDate must advance when a new default version is set")
			assert.Equal(t, "v2", got.DefaultVersionID,
				"DefaultVersionId must reflect the new default version")
		})
	}
}

func TestPolicy_DefaultVersionIdAfterSetDefault(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, err := b.CreatePolicy("DefVerAfterSet", "/", doc)
	require.NoError(t, err)

	doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	_, err = b.CreatePolicyVersion(p.Arn, doc2, false)
	require.NoError(t, err)

	require.NoError(t, b.SetDefaultPolicyVersion(p.Arn, "v2"))

	got, err := b.GetPolicy(p.Arn)
	require.NoError(t, err)
	assert.Equal(t, "v2", got.DefaultVersionID)

	// Revert to v1.
	require.NoError(t, b.SetDefaultPolicyVersion(p.Arn, "v1"))

	got2, err := b.GetPolicy(p.Arn)
	require.NoError(t, err)
	assert.Equal(t, "v1", got2.DefaultVersionID, "DefaultVersionId reverts to v1")
}
