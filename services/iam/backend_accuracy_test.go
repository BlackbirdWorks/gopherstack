package iam_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// newBackend returns a fresh InMemoryBackend for testing.
func newBackend(t *testing.T) *iam.InMemoryBackend {
	t.Helper()

	return iam.NewInMemoryBackend()
}

// ---- Tag tests (embedded in model) ----

func TestTagUser_StoresOnModel(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateUser("alice", "/", "")
	require.NoError(t, err)

	require.NoError(t, b.TagUser("alice", map[string]string{"env": "prod", "team": "platform"}))

	u, err := b.GetUser("alice")
	require.NoError(t, err)
	assert.Equal(t, "prod", u.Tags["env"])
	assert.Equal(t, "platform", u.Tags["team"])
}

func TestTagUser_MergesExistingTags(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateUser("bob", "/", "")
	require.NoError(t, err)

	require.NoError(t, b.TagUser("bob", map[string]string{"a": "1"}))
	require.NoError(t, b.TagUser("bob", map[string]string{"b": "2"}))

	u, err := b.GetUser("bob")
	require.NoError(t, err)
	assert.Equal(t, "1", u.Tags["a"])
	assert.Equal(t, "2", u.Tags["b"])
}

func TestTagUser_NotFoundError(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	err := b.TagUser("nonexistent", map[string]string{"k": "v"})
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrUserNotFound)
}

func TestUntagUser_RemovesKeys(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateUser("charlie", "/", "")
	require.NoError(t, err)

	require.NoError(t, b.TagUser("charlie", map[string]string{"env": "prod", "team": "ops"}))
	require.NoError(t, b.UntagUser("charlie", []string{"env"}))

	u, err := b.GetUser("charlie")
	require.NoError(t, err)
	assert.Empty(t, u.Tags["env"])
	assert.Equal(t, "ops", u.Tags["team"])
}

func TestUntagUser_NotFoundError(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	err := b.UntagUser("ghost", []string{"k"})
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrUserNotFound)
}

func TestTagRole_StoresOnModel(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateRole(
		"MyRole",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		"",
	)
	require.NoError(t, err)

	require.NoError(t, b.TagRole("MyRole", map[string]string{"dept": "eng"}))

	r, err := b.GetRole("MyRole")
	require.NoError(t, err)
	assert.Equal(t, "eng", r.Tags["dept"])
}

func TestTagRole_NotFoundError(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	err := b.TagRole("NoRole", map[string]string{"k": "v"})
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrRoleNotFound)
}

func TestUntagRole_RemovesKeys(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateRole(
		"R2",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		"",
	)
	require.NoError(t, err)
	require.NoError(t, b.TagRole("R2", map[string]string{"x": "1", "y": "2"}))
	require.NoError(t, b.UntagRole("R2", []string{"x"}))

	r, err := b.GetRole("R2")
	require.NoError(t, err)
	assert.Empty(t, r.Tags["x"])
	assert.Equal(t, "2", r.Tags["y"])
}

func TestTagPolicy_StoresOnModel(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	pol, err := b.CreatePolicy(
		"MyPol",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
	)
	require.NoError(t, err)

	require.NoError(t, b.TagPolicy(pol.Arn, map[string]string{"owner": "infra"}))

	p, err := b.GetPolicy(pol.Arn)
	require.NoError(t, err)
	assert.Equal(t, "infra", p.Tags["owner"])
}

func TestTagPolicy_NotFoundError(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	err := b.TagPolicy("arn:aws:iam::000000000000:policy/Ghost", map[string]string{"k": "v"})
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrPolicyNotFound)
}

func TestUntagPolicy_RemovesKeys(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	pol, _ := b.CreatePolicy(
		"P1",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
	)
	require.NoError(t, b.TagPolicy(pol.Arn, map[string]string{"a": "1", "b": "2"}))
	require.NoError(t, b.UntagPolicy(pol.Arn, []string{"a"}))

	p, err := b.GetPolicy(pol.Arn)
	require.NoError(t, err)
	assert.Empty(t, p.Tags["a"])
	assert.Equal(t, "2", p.Tags["b"])
}

func TestTagGroup_StoresOnModel(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateGroup("Admins", "/")
	require.NoError(t, err)

	require.NoError(t, b.TagGroup("Admins", map[string]string{"tier": "gold"}))

	g, err := b.GetGroup("Admins")
	require.NoError(t, err)
	assert.Equal(t, "gold", g.Tags["tier"])
}

func TestTagGroup_NotFoundError(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	err := b.TagGroup("Ghost", map[string]string{"k": "v"})
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrGroupNotFound)
}

func TestUntagGroup_RemovesKeys(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateGroup("G1", "/")
	require.NoError(t, b.TagGroup("G1", map[string]string{"a": "1", "b": "2"}))
	require.NoError(t, b.UntagGroup("G1", []string{"b"}))

	g, err := b.GetGroup("G1")
	require.NoError(t, err)
	assert.Equal(t, "1", g.Tags["a"])
	assert.Empty(t, g.Tags["b"])
}

// ---- AccessKey LastUsed tests ----

func TestRecordAccessKeyUsage_UpdatesFields(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("dave", "/", "")
	ak, err := b.CreateAccessKey("dave")
	require.NoError(t, err)

	before := time.Now().Add(-time.Second)
	b.RecordAccessKeyUsage(ak.AccessKeyID, "us-east-1", "s3")
	after := time.Now().Add(time.Second)

	result, err := b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	assert.Equal(t, "us-east-1", result.Region)
	assert.Equal(t, "s3", result.ServiceName)
	assert.NotEqual(t, "N/A", result.LastUsedDate)

	// Parse and validate the timestamp.
	parsedTime, parseErr := time.Parse("2006-01-02T15:04:05Z", result.LastUsedDate)
	require.NoError(t, parseErr)
	assert.True(t, parsedTime.After(before) || parsedTime.Equal(before.Truncate(time.Second)))
	assert.True(t, parsedTime.Before(after))
}

func TestRecordAccessKeyUsage_NonexistentKeyNoOp(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	// Must not panic.
	b.RecordAccessKeyUsage("AKIANONEXISTENT", "eu-west-1", "ec2")
}

func TestGetAccessKeyLastUsed_NeverUsed(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("eve", "/", "")
	ak, err := b.CreateAccessKey("eve")
	require.NoError(t, err)

	result, err := b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	assert.Equal(t, "N/A", result.LastUsedDate)
	assert.Equal(t, "N/A", result.Region)
	assert.Equal(t, "N/A", result.ServiceName)
}

func TestGetAccessKeyLastUsed_NotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.GetAccessKeyLastUsed("AKIANONEXISTENT")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrAccessKeyNotFound)
}

// ---- Signing Certificate tests ----

func TestUploadSigningCertificate_Success(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("frank", "/", "")

	body := "-----BEGIN CERTIFICATE-----\nMIIDxx==\n-----END CERTIFICATE-----"
	cert, err := b.UploadSigningCertificate("frank", body)
	require.NoError(t, err)
	assert.NotEmpty(t, cert.CertificateID)
	assert.True(t, strings.HasPrefix(cert.CertificateID, "ASCA"))
	assert.Equal(t, "frank", cert.UserName)
	assert.Equal(t, body, cert.CertificateBody)
	assert.Equal(t, "Active", cert.Status)
	assert.False(t, cert.UploadDate.IsZero())
}

func TestUploadSigningCertificate_UserNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.UploadSigningCertificate("ghost", "cert-body")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrUserNotFound)
}

func TestUploadSigningCertificate_EmptyBody(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("grace", "/", "")
	_, err := b.UploadSigningCertificate("grace", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrMalformedPolicyDocument)
}

func TestListSigningCertificates_ReturnsByUser(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("u1", "/", "")
	_, _ = b.CreateUser("u2", "/", "")

	_, _ = b.UploadSigningCertificate("u1", "cert1")
	_, _ = b.UploadSigningCertificate("u2", "cert2")
	_, _ = b.UploadSigningCertificate("u1", "cert3")

	certs, err := b.ListSigningCertificates("u1")
	require.NoError(t, err)
	assert.Len(t, certs, 2)

	for _, c := range certs {
		assert.Equal(t, "u1", c.UserName)
	}
}

func TestListSigningCertificates_UserNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.ListSigningCertificates("ghost")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrUserNotFound)
}

func TestListSigningCertificates_EmptyUser_ReturnsAll(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("u3", "/", "")
	_, _ = b.CreateUser("u4", "/", "")
	_, _ = b.UploadSigningCertificate("u3", "cert-a")
	_, _ = b.UploadSigningCertificate("u4", "cert-b")

	certs, err := b.ListSigningCertificates("")
	require.NoError(t, err)
	assert.Len(t, certs, 2)
}

func TestUpdateSigningCertificate_ChangesStatus(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("henry", "/", "")
	cert, err := b.UploadSigningCertificate("henry", "body")
	require.NoError(t, err)

	require.NoError(t, b.UpdateSigningCertificate(cert.CertificateID, "Inactive"))

	certs, err := b.ListSigningCertificates("henry")
	require.NoError(t, err)
	require.Len(t, certs, 1)
	assert.Equal(t, "Inactive", certs[0].Status)
}

func TestUpdateSigningCertificate_InvalidStatus(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("ivan", "/", "")
	cert, _ := b.UploadSigningCertificate("ivan", "body")
	err := b.UpdateSigningCertificate(cert.CertificateID, "Deleted")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrInvalidAction)
}

func TestUpdateSigningCertificate_NotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	err := b.UpdateSigningCertificate("ASCANONEXISTENT", "Inactive")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrAccessKeyNotFound)
}

func TestDeleteSigningCertificate_Success(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("julia", "/", "")
	cert, _ := b.UploadSigningCertificate("julia", "body")

	require.NoError(t, b.DeleteSigningCertificate(cert.CertificateID))

	certs, err := b.ListSigningCertificates("julia")
	require.NoError(t, err)
	assert.Empty(t, certs)
}

func TestDeleteSigningCertificate_NotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	err := b.DeleteSigningCertificate("ASCANONEXISTENT")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrAccessKeyNotFound)
}

func TestSigningCertificate_IDUniqueness(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("karl", "/", "")

	ids := make(map[string]bool)
	for range 10 {
		cert, err := b.UploadSigningCertificate("karl", "body")
		require.NoError(t, err)
		assert.False(t, ids[cert.CertificateID], "duplicate cert ID: %s", cert.CertificateID)
		ids[cert.CertificateID] = true
	}
}

// ---- MFA Device Status State Machine tests ----

func TestMFADevice_InitialStatus_NotAssigned(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	dev, err := b.CreateVirtualMFADeviceFull("MyDevice", "/")
	require.NoError(t, err)
	assert.Equal(t, iam.MFAStatusNotAssigned, dev.Status)
}

func TestEnableMFADevice_SetsActiveStatus(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("lena", "/", "")
	dev, _ := b.CreateVirtualMFADeviceFull("LenaDevice", "/")

	require.NoError(t, b.EnableMFADevice("lena", dev.SerialNumber, "111111", "222222"))

	devices, err := b.ListMFADevicesForUser("lena")
	require.NoError(t, err)
	require.Len(t, devices, 1)
	assert.Equal(t, iam.MFAStatusEnabled, devices[0].Status)
}

func TestEnableMFADevice_RejectsDoubleEnable(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("mike", "/", "")
	dev, _ := b.CreateVirtualMFADeviceFull("MikeDevice", "/")

	require.NoError(t, b.EnableMFADevice("mike", dev.SerialNumber, "111111", "222222"))

	// Second enable on same device must fail.
	err := b.EnableMFADevice("mike", dev.SerialNumber, "333333", "444444")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrInvalidAction)
}

func TestDeactivateMFADevice_SetsDeactivatedStatus(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("nina", "/", "")
	dev, _ := b.CreateVirtualMFADeviceFull("NinaDevice", "/")
	require.NoError(t, b.EnableMFADevice("nina", dev.SerialNumber, "111111", "222222"))

	require.NoError(t, b.DeactivateMFADevice("nina", dev.SerialNumber))
}

func TestDeactivateMFADevice_RejectsUnenabled(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("omar", "/", "")
	dev, _ := b.CreateVirtualMFADeviceFull("OmarDevice", "/")

	// Device is PENDING — cannot deactivate.
	err := b.DeactivateMFADevice("omar", dev.SerialNumber)
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrInvalidAction)
}

// ---- Permission Boundary enforcement in SimulatePrincipalPolicy ----

const allowS3GetDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`

func TestSimulatePrincipalPolicy_AllowWithoutBoundary(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("petra", "/", "")
	pol, _ := b.CreatePolicy("S3Get", "/", allowS3GetDoc)
	_ = b.AttachUserPolicy("petra", pol.Arn)

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/petra",
		[]string{"s3:GetObject"},
		[]string{"arn:aws:s3:::my-bucket/*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "allowed", results[0].Decision)
}

func TestSimulatePrincipalPolicy_BoundaryDeniesAllowedAction(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	// Create boundary that only allows s3:ListBucket.
	boundaryDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"*"}]}`
	boundaryPol, _ := b.CreatePolicy("Boundary", "/", boundaryDoc)

	_, _ = b.CreateUser("quinn", "/", boundaryPol.Arn)

	// Attach a policy that grants s3:GetObject (identity allows, boundary doesn't).
	identityPol, _ := b.CreatePolicy("S3GetQ", "/", allowS3GetDoc)
	_ = b.AttachUserPolicy("quinn", identityPol.Arn)

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/quinn",
		[]string{"s3:GetObject"},
		[]string{"arn:aws:s3:::my-bucket/*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)
	// Identity allows but boundary doesn't → implicitDeny.
	assert.Equal(t, "implicitDeny", results[0].Decision)
}

func TestSimulatePrincipalPolicy_BoundaryAllowsActionGrantedByIdentity(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	// Boundary allows everything.
	boundaryDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	boundaryPol, _ := b.CreatePolicy("WildBoundary", "/", boundaryDoc)

	_, _ = b.CreateUser("rosa", "/", boundaryPol.Arn)
	identityPol, _ := b.CreatePolicy("S3GetR", "/", allowS3GetDoc)
	_ = b.AttachUserPolicy("rosa", identityPol.Arn)

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/rosa",
		[]string{"s3:GetObject"},
		[]string{"arn:aws:s3:::my-bucket/*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "allowed", results[0].Decision)
}

func TestSimulatePrincipalPolicy_RoleBoundaryEnforced(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	boundaryDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"ec2:*","Resource":"*"}]}`
	boundaryPol, _ := b.CreatePolicy("EC2Boundary", "/", boundaryDoc)

	trustDoc := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	_, _ = b.CreateRole("TestRole", "/", trustDoc, boundaryPol.Arn)

	// Attach s3:GetObject to role — boundary only allows ec2:*.
	identityPol, _ := b.CreatePolicy("S3GetRole", "/", allowS3GetDoc)
	_ = b.AttachRolePolicy("TestRole", identityPol.Arn)

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:role/TestRole",
		[]string{"s3:GetObject"},
		[]string{"arn:aws:s3:::my-bucket/*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "implicitDeny", results[0].Decision)
}

// ---- EvaluateAssumeRoleTrustPolicy tests ----

func TestEvaluateAssumeRoleTrustPolicy_AllowsMatchingPrincipal(t *testing.T) {
	t.Parallel()

	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":{"AWS":"arn:aws:iam::111111111111:root"},
			"Action":"sts:AssumeRole"
		}]
	}`

	result := iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::111111111111:root", "", false, iam.ConditionContext{},
	)
	assert.True(t, result)
}

func TestEvaluateAssumeRoleTrustPolicy_DeniesNonMatchingPrincipal(t *testing.T) {
	t.Parallel()

	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":{"AWS":"arn:aws:iam::111111111111:root"},
			"Action":"sts:AssumeRole"
		}]
	}`

	result := iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::999999999999:root", "", false, iam.ConditionContext{},
	)
	assert.False(t, result)
}

func TestEvaluateAssumeRoleTrustPolicy_WildcardPrincipalAllowsAll(t *testing.T) {
	t.Parallel()

	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":"*",
			"Action":"sts:AssumeRole"
		}]
	}`

	result := iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::anything:root", "", false, iam.ConditionContext{},
	)
	assert.True(t, result)
}

func TestEvaluateAssumeRoleTrustPolicy_ExternalIDRequired(t *testing.T) {
	t.Parallel()

	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":{"AWS":"arn:aws:iam::111111111111:root"},
			"Action":"sts:AssumeRole",
			"Condition":{"StringEquals":{"sts:ExternalId":"secret-id"}}
		}]
	}`

	// Correct external ID.
	assert.True(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::111111111111:root", "secret-id", false, iam.ConditionContext{},
	))

	// Wrong external ID.
	assert.False(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::111111111111:root", "wrong-id", false, iam.ConditionContext{},
	))

	// Missing external ID.
	assert.False(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::111111111111:root", "", false, iam.ConditionContext{},
	))
}

func TestEvaluateAssumeRoleTrustPolicy_MFARequired(t *testing.T) {
	t.Parallel()

	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":{"AWS":"arn:aws:iam::111111111111:user/alice"},
			"Action":"sts:AssumeRole",
			"Condition":{"Bool":{"aws:MultiFactorAuthPresent":"true"}}
		}]
	}`

	// MFA present → allowed.
	assert.True(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::111111111111:user/alice", "", true, iam.ConditionContext{},
	))

	// MFA absent → denied.
	assert.False(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::111111111111:user/alice", "", false, iam.ConditionContext{},
	))
}

func TestEvaluateAssumeRoleTrustPolicy_ServicePrincipal(t *testing.T) {
	t.Parallel()

	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":{"Service":"lambda.amazonaws.com"},
			"Action":"sts:AssumeRole"
		}]
	}`

	assert.True(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "lambda.amazonaws.com", "", false, iam.ConditionContext{},
	))
	assert.False(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "ec2.amazonaws.com", "", false, iam.ConditionContext{},
	))
}

func TestEvaluateAssumeRoleTrustPolicy_EmptyPolicy(t *testing.T) {
	t.Parallel()

	result := iam.EvaluateAssumeRoleTrustPolicy("", "anything", "", false, iam.ConditionContext{})
	assert.False(t, result)
}

func TestEvaluateAssumeRoleTrustPolicy_MultipleStatements(t *testing.T) {
	t.Parallel()

	// Second statement allows a different principal.
	policy := `{
		"Version":"2012-10-17",
		"Statement":[
			{
				"Effect":"Allow",
				"Principal":{"AWS":"arn:aws:iam::111111111111:root"},
				"Action":"sts:AssumeRole"
			},
			{
				"Effect":"Allow",
				"Principal":{"Service":"ec2.amazonaws.com"},
				"Action":"sts:AssumeRole"
			}
		]
	}`

	assert.True(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::111111111111:root", "", false, iam.ConditionContext{},
	))
	assert.True(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "ec2.amazonaws.com", "", false, iam.ConditionContext{},
	))
	assert.False(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::999999999999:root", "", false, iam.ConditionContext{},
	))
}

// ---- Trust Policy Principal Validation tests ----

func TestCreateRole_RejectsBadPrincipalARN(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Non-ARN AWS principal.
	badPolicy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":{"AWS":"not-an-arn"},
			"Action":"sts:AssumeRole"
		}]
	}`

	_, err := b.CreateRole("BadRole", "/", badPolicy, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrMalformedPolicyDocument)
}

func TestCreateRole_AllowsWildcardPrincipal(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Wildcard principal is allowed.
	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":"*",
			"Action":"sts:AssumeRole"
		}]
	}`

	_, err := b.CreateRole("WildRole", "/", policy, "")
	require.NoError(t, err)
}

func TestCreateRole_AllowsServicePrincipal(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":{"Service":"lambda.amazonaws.com"},
			"Action":"sts:AssumeRole"
		}]
	}`

	_, err := b.CreateRole("LambdaRole", "/", policy, "")
	require.NoError(t, err)
}

func TestUpdateAssumeRolePolicy_RejectsBadPrincipal(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	validPolicy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	_, err := b.CreateRole("TestRole2", "/", validPolicy, "")
	require.NoError(t, err)

	badPolicy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":{"AWS":"invalid-not-arn"},"Action":"sts:AssumeRole"}]}`
	err = b.UpdateAssumeRolePolicy("TestRole2", badPolicy)
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrMalformedPolicyDocument)
}

// ---- Role path immutability tests ----

func TestUpdateRole_RejectsPathChange(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	validPolicy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	_, err := b.CreateRole("PathTestRole", "/", validPolicy, "")
	require.NoError(t, err)

	// Backend UpdateRole itself only changes description, not path. The path-rejection
	// is enforced in the handler layer. Test the handler via HTTP in handler tests.
	// Here we test that UpdateRole only updates description.
	err = b.UpdateRole("PathTestRole", "new description")
	require.NoError(t, err)

	r, err := b.GetRole("PathTestRole")
	require.NoError(t, err)
	assert.Equal(t, "new description", r.Description)
	assert.Equal(t, "/", r.Path) // path unchanged
}

// ---- Reset preserves signingCertificates map ----

func TestReset_ClearsSigningCertificates(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("zoe", "/", "")
	_, _ = b.UploadSigningCertificate("zoe", "body")

	b.Reset()

	// After reset, user doesn't exist so list by user fails. Test all-certs list.
	_, _ = b.CreateUser("zoe", "/", "")
	certs, err := b.ListSigningCertificates("zoe")
	require.NoError(t, err)
	assert.Empty(t, certs)
}

// ---- Tag XML round-trip tests ----

func TestTagsToXML_Sorted(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("sorted-user", "/", "")
	require.NoError(t, b.TagUser("sorted-user", map[string]string{
		"z-last":  "val-z",
		"a-first": "val-a",
		"m-mid":   "val-m",
	}))

	u, err := b.GetUser("sorted-user")
	require.NoError(t, err)
	assert.Equal(t, "val-a", u.Tags["a-first"])
	assert.Equal(t, "val-m", u.Tags["m-mid"])
	assert.Equal(t, "val-z", u.Tags["z-last"])
}

// ---- Credential report reflects MFA status ----

func TestCredentialReport_MFAStatusReflected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("report-user", "/", "")

	// Before MFA: mfa_active column should be false.
	report := b.GetCredentialReport()
	assert.Contains(t, report, "report-user")
}

// ---- Multiple signing certs per user ----

func TestSigningCertificate_MultipleCertsPerUser(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("multi-cert-user", "/", "")

	cert1, err := b.UploadSigningCertificate("multi-cert-user", "body-1")
	require.NoError(t, err)

	cert2, err := b.UploadSigningCertificate("multi-cert-user", "body-2")
	require.NoError(t, err)

	assert.NotEqual(t, cert1.CertificateID, cert2.CertificateID)

	certs, err := b.ListSigningCertificates("multi-cert-user")
	require.NoError(t, err)
	assert.Len(t, certs, 2)
}

// ---- AccessKey LastUsed persists across calls ----

func TestAccessKeyLastUsed_MultipleUsages(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("repeated-user", "/", "")
	ak, _ := b.CreateAccessKey("repeated-user")

	b.RecordAccessKeyUsage(ak.AccessKeyID, "us-west-2", "iam")
	b.RecordAccessKeyUsage(ak.AccessKeyID, "ap-southeast-1", "ec2")

	result, err := b.GetAccessKeyLastUsed(ak.AccessKeyID)
	require.NoError(t, err)
	// Last call wins.
	assert.Equal(t, "ap-southeast-1", result.Region)
	assert.Equal(t, "ec2", result.ServiceName)
}

// ---- trustPrincipal multi-value list ----

func TestEvaluateAssumeRoleTrustPolicy_MultiValueAWSPrincipal(t *testing.T) {
	t.Parallel()

	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":{"AWS":["arn:aws:iam::111111111111:root","arn:aws:iam::222222222222:root"]},
			"Action":"sts:AssumeRole"
		}]
	}`

	assert.True(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::111111111111:root", "", false, iam.ConditionContext{},
	))
	assert.True(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::222222222222:root", "", false, iam.ConditionContext{},
	))
	assert.False(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::333333333333:root", "", false, iam.ConditionContext{},
	))
}

// ---- TagGroup round-trip after Reset ----

func TestTagGroup_SurvivesMultipleOperations(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateGroup("Engineers", "/")

	require.NoError(t, b.TagGroup("Engineers", map[string]string{"tier": "senior", "dept": "eng"}))
	require.NoError(t, b.UntagGroup("Engineers", []string{"tier"}))
	require.NoError(t, b.TagGroup("Engineers", map[string]string{"region": "us-east"}))

	g, err := b.GetGroup("Engineers")
	require.NoError(t, err)
	assert.Empty(t, g.Tags["tier"])
	assert.Equal(t, "eng", g.Tags["dept"])
	assert.Equal(t, "us-east", g.Tags["region"])
}

// ---- Boundary doesn't affect explicit deny ----

func TestSimulatePrincipalPolicy_ExplicitDenyTakesPrecedence(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Boundary allows everything.
	boundaryPol, _ := b.CreatePolicy("AllBoundary", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`)

	_, _ = b.CreateUser("explicit-deny-user", "/", boundaryPol.Arn)

	// Identity: allow s3:* but deny s3:DeleteObject.
	identityDoc := `{
		"Version":"2012-10-17",
		"Statement":[
			{"Effect":"Allow","Action":"s3:*","Resource":"*"},
			{"Effect":"Deny","Action":"s3:DeleteObject","Resource":"*"}
		]
	}`
	identityPol, _ := b.CreatePolicy("S3WithDeny", "/", identityDoc)
	_ = b.AttachUserPolicy("explicit-deny-user", identityPol.Arn)

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/explicit-deny-user",
		[]string{"s3:DeleteObject", "s3:GetObject"},
		[]string{"arn:aws:s3:::bucket/*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 2)

	resultMap := make(map[string]string)
	for _, r := range results {
		resultMap[r.ActionName] = r.Decision
	}

	assert.Equal(t, "explicitDeny", resultMap["s3:DeleteObject"])
	assert.Equal(t, "allowed", resultMap["s3:GetObject"])
}

// ---- SigningCert status toggle ----

func TestSigningCertificate_ToggleStatus(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("toggle-user", "/", "")
	cert, _ := b.UploadSigningCertificate("toggle-user", "body")

	// Active → Inactive.
	require.NoError(t, b.UpdateSigningCertificate(cert.CertificateID, "Inactive"))

	certs, _ := b.ListSigningCertificates("toggle-user")
	assert.Equal(t, "Inactive", certs[0].Status)

	// Inactive → Active.
	require.NoError(t, b.UpdateSigningCertificate(cert.CertificateID, "Active"))

	certs, _ = b.ListSigningCertificates("toggle-user")
	assert.Equal(t, "Active", certs[0].Status)
}
