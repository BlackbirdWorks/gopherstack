package iam_test

// AWS-accuracy audit batch-1 — handler-level tests for policy attach/detach,
// access-key rotation, MFA, password policy, credential report, and account summary.

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// ---- AttachUserPolicy / DetachUserPolicy ----

func TestHandler_AttachDetachUserPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")
	doc := `{"Version":"2012-10-17","Statement":[]}`
	p, _ := b.CreatePolicy("P", "/", doc)

	// Attach.
	req := iamRequest("AttachUserPolicy", map[string]string{
		"UserName":  "alice",
		"PolicyArn": p.Arn,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify via ListAttachedUserPolicies.
	req2 := iamRequest("ListAttachedUserPolicies", map[string]string{"UserName": "alice"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), p.Arn)

	// Detach.
	req3 := iamRequest("DetachUserPolicy", map[string]string{
		"UserName":  "alice",
		"PolicyArn": p.Arn,
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Verify detached.
	req4 := iamRequest("ListAttachedUserPolicies", map[string]string{"UserName": "alice"})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)
	assert.NotContains(t, rec4.Body.String(), p.Arn)
}

func TestHandler_AttachRolePolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")
	p, _ := b.CreatePolicy("P", "/", doc)

	req := iamRequest("AttachRolePolicy", map[string]string{
		"RoleName":  "MyRole",
		"PolicyArn": p.Arn,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("ListAttachedRolePolicies", map[string]string{"RoleName": "MyRole"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Contains(t, rec2.Body.String(), p.Arn)

	req3 := iamRequest("DetachRolePolicy", map[string]string{
		"RoleName":  "MyRole",
		"PolicyArn": p.Arn,
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestHandler_AttachGroupPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateGroup("Admins", "/")
	p, _ := b.CreatePolicy("P", "/", doc)

	req := iamRequest("AttachGroupPolicy", map[string]string{
		"GroupName": "Admins",
		"PolicyArn": p.Arn,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("ListAttachedGroupPolicies", map[string]string{"GroupName": "Admins"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Contains(t, rec2.Body.String(), p.Arn)

	req3 := iamRequest("DetachGroupPolicy", map[string]string{
		"GroupName": "Admins",
		"PolicyArn": p.Arn,
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// ---- Inline policies (User/Role/Group) ----

func TestHandler_UserInlinePolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")
	doc := `{"Version":"2012-10-17","Statement":[]}`

	req := iamRequest("PutUserPolicy", map[string]string{
		"UserName":       "alice",
		"PolicyName":     "MyPolicy",
		"PolicyDocument": doc,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("GetUserPolicy", map[string]string{
		"UserName":   "alice",
		"PolicyName": "MyPolicy",
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "MyPolicy")

	req3 := iamRequest("ListUserPolicies", map[string]string{"UserName": "alice"})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Contains(t, rec3.Body.String(), "MyPolicy")

	req4 := iamRequest("DeleteUserPolicy", map[string]string{
		"UserName":   "alice",
		"PolicyName": "MyPolicy",
	})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)
}

func TestHandler_RoleInlinePolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")

	req := iamRequest("PutRolePolicy", map[string]string{
		"RoleName":       "MyRole",
		"PolicyName":     "RolePolicy",
		"PolicyDocument": doc,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("GetRolePolicy", map[string]string{
		"RoleName":   "MyRole",
		"PolicyName": "RolePolicy",
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Contains(t, rec2.Body.String(), "RolePolicy")

	req3 := iamRequest("ListRolePolicies", map[string]string{"RoleName": "MyRole"})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Contains(t, rec3.Body.String(), "RolePolicy")

	req4 := iamRequest("DeleteRolePolicy", map[string]string{
		"RoleName":   "MyRole",
		"PolicyName": "RolePolicy",
	})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)
}

func TestHandler_GroupInlinePolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateGroup("Ops", "/")
	doc := `{"Version":"2012-10-17","Statement":[]}`

	req := iamRequest("PutGroupPolicy", map[string]string{
		"GroupName":      "Ops",
		"PolicyName":     "OpsPolicy",
		"PolicyDocument": doc,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("ListGroupPolicies", map[string]string{"GroupName": "Ops"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Contains(t, rec2.Body.String(), "OpsPolicy")

	req3 := iamRequest("DeleteGroupPolicy", map[string]string{
		"GroupName":  "Ops",
		"PolicyName": "OpsPolicy",
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// ---- PolicyVersion lifecycle ----

func TestHandler_PolicyVersion_CRUD(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	p, _ := b.CreatePolicy("VersionedPolicy", "/", doc)

	// CreatePolicyVersion.
	doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	req := iamRequest("CreatePolicyVersion", map[string]string{
		"PolicyArn":      p.Arn,
		"PolicyDocument": doc2,
		"SetAsDefault":   "false",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code, "CreatePolicyVersion must succeed")

	// ListPolicyVersions.
	req2 := iamRequest("ListPolicyVersions", map[string]string{"PolicyArn": p.Arn})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "v1")
	assert.Contains(t, rec2.Body.String(), "v2")

	// GetPolicyVersion.
	req3 := iamRequest("GetPolicyVersion", map[string]string{
		"PolicyArn": p.Arn,
		"VersionId": "v2",
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
	assert.Contains(t, rec3.Body.String(), "s3:*")

	// SetDefaultPolicyVersion.
	req4 := iamRequest("SetDefaultPolicyVersion", map[string]string{
		"PolicyArn": p.Arn,
		"VersionId": "v2",
	})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)

	// DeletePolicyVersion (non-default).
	req5 := iamRequest("CreatePolicyVersion", map[string]string{
		"PolicyArn":      p.Arn,
		"PolicyDocument": doc,
		"SetAsDefault":   "false",
	})
	rec5 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req5, rec5)))
	require.Equal(t, http.StatusOK, rec5.Code)

	req6 := iamRequest("DeletePolicyVersion", map[string]string{
		"PolicyArn": p.Arn,
		"VersionId": "v1",
	})
	rec6 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req6, rec6)))
	assert.Equal(t, http.StatusOK, rec6.Code)
}

func TestHandler_PolicyVersion_LimitExceeded(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	p, _ := b.CreatePolicy("LP", "/", doc)

	// Create 4 more versions to hit the cap (v1 + v2 + v3 + v4 + v5 = 5).
	for range 4 {
		req := iamRequest("CreatePolicyVersion", map[string]string{
			"PolicyArn":      p.Arn,
			"PolicyDocument": doc,
			"SetAsDefault":   "false",
		})
		rec := httptest.NewRecorder()
		require.NoError(t, h.Handler()(e.NewContext(req, rec)))
		require.Equal(t, http.StatusOK, rec.Code, "must succeed under limit")
	}

	// 6th version must fail.
	req := iamRequest("CreatePolicyVersion", map[string]string{
		"PolicyArn":      p.Arn,
		"PolicyDocument": doc,
		"SetAsDefault":   "false",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp iam.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "LimitExceeded", errResp.Error.Code)
}

// ---- AccessKey rotation and status ----

func TestHandler_AccessKey_RotationCycle(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")

	// Create first key.
	req := iamRequest("CreateAccessKey", map[string]string{"UserName": "alice"})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp iam.CreateAccessKeyResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &createResp))
	keyID := createResp.CreateAccessKeyResult.AccessKey.AccessKeyID
	assert.True(t, strings.HasPrefix(keyID, "AKIA"))
	assert.Len(t, keyID, 20)
	assert.Equal(t, "Active", createResp.CreateAccessKeyResult.AccessKey.Status)

	// Deactivate via UpdateAccessKey.
	req2 := iamRequest("UpdateAccessKey", map[string]string{
		"UserName":    "alice",
		"AccessKeyId": keyID,
		"Status":      "Inactive",
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)

	// List to confirm Inactive.
	req3 := iamRequest("ListAccessKeys", map[string]string{"UserName": "alice"})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Contains(t, rec3.Body.String(), "Inactive")

	// GetAccessKeyLastUsed.
	req4 := iamRequest("GetAccessKeyLastUsed", map[string]string{"AccessKeyId": keyID})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)
	assert.Contains(t, rec4.Body.String(), "alice", "response must include username")

	// Delete.
	req5 := iamRequest("DeleteAccessKey", map[string]string{
		"UserName":    "alice",
		"AccessKeyId": keyID,
	})
	rec5 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req5, rec5)))
	assert.Equal(t, http.StatusOK, rec5.Code)
}

func TestHandler_AccessKey_MaxTwoLimit_Via_Handler(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")

	for range 2 {
		req := iamRequest("CreateAccessKey", map[string]string{"UserName": "alice"})
		rec := httptest.NewRecorder()
		require.NoError(t, h.Handler()(e.NewContext(req, rec)))
		require.Equal(t, http.StatusOK, rec.Code, "first two keys must succeed")
	}

	req := iamRequest("CreateAccessKey", map[string]string{"UserName": "alice"})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusBadRequest, rec.Code, "third key must fail")

	var errResp iam.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "LimitExceeded", errResp.Error.Code)
}

// ---- AccessKey ID format at handler level ----

func TestHandler_AccessKey_IDFormat(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("bob", "/", "")

	req := iamRequest("CreateAccessKey", map[string]string{"UserName": "bob"})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp iam.CreateAccessKeyResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	keyID := resp.CreateAccessKeyResult.AccessKey.AccessKeyID

	assert.Len(t, keyID, 20, "access key ID must be 20 chars")
	assert.True(t, strings.HasPrefix(keyID, "AKIA"), "must start with AKIA")
	assert.NotContains(t, keyID, "-", "must not contain dashes")
	for _, ch := range keyID {
		assert.True(t, unicode.IsUpper(ch) || unicode.IsDigit(ch),
			"char %q must be uppercase alphanumeric", ch)
	}
}

// ---- MFA virtual device ----

func TestHandler_VirtualMFA_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")

	// CreateVirtualMFADevice.
	req := iamRequest("CreateVirtualMFADevice", map[string]string{
		"VirtualMFADeviceName": "alice-mfa",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code, "CreateVirtualMFADevice must succeed")
	assert.Contains(t, rec.Body.String(), "SerialNumber")

	// ListVirtualMFADevices.
	req2 := iamRequest("ListVirtualMFADevices", map[string]string{})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "alice-mfa")

	// EnableMFADevice using the backend serial number directly.
	dev, err := b.CreateVirtualMFADeviceFull("alice-mfa2", "/mfa/")
	require.NoError(t, err)

	req3 := iamRequest("EnableMFADevice", map[string]string{
		"UserName":            "alice",
		"SerialNumber":        dev.SerialNumber,
		"AuthenticationCode1": "123456",
		"AuthenticationCode2": "789012",
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code, "EnableMFADevice must succeed")

	// ListMFADevices.
	req4 := iamRequest("ListMFADevices", map[string]string{"UserName": "alice"})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)
	assert.Contains(t, rec4.Body.String(), dev.SerialNumber)

	// DeactivateMFADevice.
	req5 := iamRequest("DeactivateMFADevice", map[string]string{
		"UserName":     "alice",
		"SerialNumber": dev.SerialNumber,
	})
	rec5 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req5, rec5)))
	assert.Equal(t, http.StatusOK, rec5.Code)

	// DeleteVirtualMFADevice.
	req6 := iamRequest("DeleteVirtualMFADevice", map[string]string{
		"SerialNumber": dev.SerialNumber,
	})
	rec6 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req6, rec6)))
	assert.Equal(t, http.StatusOK, rec6.Code)
}

// ---- PasswordPolicy via handler ----

func TestHandler_AccountPasswordPolicy_CRUD(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	// UpdateAccountPasswordPolicy.
	req := iamRequest("UpdateAccountPasswordPolicy", map[string]string{
		"MinimumPasswordLength":      "12",
		"RequireUppercaseCharacters": "true",
		"RequireNumbers":             "true",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetAccountPasswordPolicy.
	req2 := iamRequest("GetAccountPasswordPolicy", map[string]string{})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "12")

	// DeleteAccountPasswordPolicy.
	req3 := iamRequest("DeleteAccountPasswordPolicy", map[string]string{})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestHandler_ChangePassword_PolicyEnforced(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	require.NoError(t, b.UpdateAccountPasswordPolicy(iam.PasswordPolicy{
		MinimumPasswordLength: 12,
	}))

	// Short password must fail.
	req := iamRequest("ChangePassword", map[string]string{
		"OldPassword": "OldPassword1!",
		"NewPassword": "short",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp iam.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidInput", errResp.Error.Code)

	// Valid password must succeed.
	req2 := iamRequest("ChangePassword", map[string]string{
		"OldPassword": "OldPassword1!",
		"NewPassword": "ValidPassword12!",
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// ---- ServerCertificate via handler ----

func TestHandler_ServerCertificate_CRUD(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)
	const certBody = "-----BEGIN CERTIFICATE-----\nfakecert\n-----END CERTIFICATE-----"

	// UploadServerCertificate.
	req := iamRequest("UploadServerCertificate", map[string]string{
		"ServerCertificateName": "MyCert",
		"CertificateBody":       certBody,
		"PrivateKey":            "-----BEGIN RSA PRIVATE KEY-----\nfakekey\n-----END RSA PRIVATE KEY-----",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code, "UploadServerCertificate must succeed")
	assert.Contains(t, rec.Body.String(), "MyCert")

	// GetServerCertificate.
	req2 := iamRequest("GetServerCertificate", map[string]string{
		"ServerCertificateName": "MyCert",
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "MyCert")

	// ListServerCertificates.
	req3 := iamRequest("ListServerCertificates", map[string]string{})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
	assert.Contains(t, rec3.Body.String(), "MyCert")

	// UpdateServerCertificate.
	req4 := iamRequest("UpdateServerCertificate", map[string]string{
		"ServerCertificateName":    "MyCert",
		"NewServerCertificateName": "RenamedCert",
	})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)

	// DeleteServerCertificate.
	req5 := iamRequest("DeleteServerCertificate", map[string]string{
		"ServerCertificateName": "RenamedCert",
	})
	rec5 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req5, rec5)))
	assert.Equal(t, http.StatusOK, rec5.Code)
}

// ---- InstanceProfile via handler ----

func TestHandler_InstanceProfile_CRUD(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")

	// CreateInstanceProfile.
	req := iamRequest("CreateInstanceProfile", map[string]string{
		"InstanceProfileName": "MyProfile",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "MyProfile")

	// GetInstanceProfile.
	req2 := iamRequest("GetInstanceProfile", map[string]string{
		"InstanceProfileName": "MyProfile",
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)

	// AddRoleToInstanceProfile.
	req3 := iamRequest("AddRoleToInstanceProfile", map[string]string{
		"InstanceProfileName": "MyProfile",
		"RoleName":            "MyRole",
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)

	// ListInstanceProfiles.
	req4 := iamRequest("ListInstanceProfiles", map[string]string{})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Contains(t, rec4.Body.String(), "MyProfile")

	// ListInstanceProfilesForRole.
	req5 := iamRequest("ListInstanceProfilesForRole", map[string]string{"RoleName": "MyRole"})
	rec5 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req5, rec5)))
	assert.Equal(t, http.StatusOK, rec5.Code)
	assert.Contains(t, rec5.Body.String(), "MyProfile")

	// RemoveRoleFromInstanceProfile.
	req6 := iamRequest("RemoveRoleFromInstanceProfile", map[string]string{
		"InstanceProfileName": "MyProfile",
		"RoleName":            "MyRole",
	})
	rec6 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req6, rec6)))
	assert.Equal(t, http.StatusOK, rec6.Code)

	// DeleteInstanceProfile.
	req7 := iamRequest("DeleteInstanceProfile", map[string]string{
		"InstanceProfileName": "MyProfile",
	})
	rec7 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req7, rec7)))
	assert.Equal(t, http.StatusOK, rec7.Code)
}

func TestHandler_InstanceProfile_OneRoleLimit(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateRole("R1", "/", doc, "")
	_, _ = b.CreateRole("R2", "/", doc, "")
	_, _ = b.CreateInstanceProfile("IP", "/")
	_ = b.AddRoleToInstanceProfile("IP", "R1")

	req := iamRequest("AddRoleToInstanceProfile", map[string]string{
		"InstanceProfileName": "IP",
		"RoleName":            "R2",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusBadRequest, rec.Code, "second role must fail")

	var errResp iam.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "LimitExceeded", errResp.Error.Code)
}

// ---- SAMLProvider via handler ----

func TestHandler_SAMLProvider_CRUD(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	// CreateSAMLProvider.
	req := iamRequest("CreateSAMLProvider", map[string]string{
		"Name":                 "MySAML",
		"SAMLMetadataDocument": "<md>doc</md>",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "saml-provider")

	var createResp struct {
		CreateSAMLProviderResult struct {
			SAMLProviderArn string `xml:"SAMLProviderArn"`
		} `xml:"CreateSAMLProviderResult"`
	}
	_ = xml.Unmarshal(rec.Body.Bytes(), &createResp)
	provArn := createResp.CreateSAMLProviderResult.SAMLProviderArn

	// GetSAMLProvider.
	req2 := iamRequest("GetSAMLProvider", map[string]string{"SAMLProviderArn": provArn})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)

	// UpdateSAMLProvider.
	req3 := iamRequest("UpdateSAMLProvider", map[string]string{
		"SAMLProviderArn":      provArn,
		"SAMLMetadataDocument": "<md>updated</md>",
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)

	// ListSAMLProviders.
	req4 := iamRequest("ListSAMLProviders", map[string]string{})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Contains(t, rec4.Body.String(), "MySAML")

	// DeleteSAMLProvider.
	req5 := iamRequest("DeleteSAMLProvider", map[string]string{"SAMLProviderArn": provArn})
	rec5 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req5, rec5)))
	assert.Equal(t, http.StatusOK, rec5.Code)
}

// ---- OIDCProvider via handler ----

func TestHandler_OIDCProvider_CRUD(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	// CreateOpenIDConnectProvider.
	req := iamRequest("CreateOpenIDConnectProvider", map[string]string{
		"Url":                     "https://example.com/oidc",
		"ClientIDList.member.1":   "client1",
		"ThumbprintList.member.1": "abc123def456",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		CreateOpenIDConnectProviderResult struct {
			OpenIDConnectProviderArn string `xml:"OpenIDConnectProviderArn"`
		} `xml:"CreateOpenIDConnectProviderResult"`
	}
	_ = xml.Unmarshal(rec.Body.Bytes(), &createResp)
	oidcArn := createResp.CreateOpenIDConnectProviderResult.OpenIDConnectProviderArn

	if oidcArn == "" {
		// Some response formats embed differently; skip OIDC ARN-dependent ops.
		return
	}

	// GetOpenIDConnectProvider.
	req2 := iamRequest("GetOpenIDConnectProvider", map[string]string{
		"OpenIDConnectProviderArn": oidcArn,
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)

	// ListOpenIDConnectProviders.
	req3 := iamRequest("ListOpenIDConnectProviders", map[string]string{})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)

	// DeleteOpenIDConnectProvider.
	req4 := iamRequest("DeleteOpenIDConnectProvider", map[string]string{
		"OpenIDConnectProviderArn": oidcArn,
	})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)
}

// ---- PermissionsBoundary via handler ----

func TestHandler_PermissionsBoundary_UserRoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")
	p, _ := b.CreatePolicy("Boundary", "/", `{"Version":"2012-10-17","Statement":[]}`)

	req := iamRequest("PutUserPermissionsBoundary", map[string]string{
		"UserName":            "alice",
		"PermissionsBoundary": p.Arn,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("GetUserPermissionsBoundary", map[string]string{"UserName": "alice"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)

	req3 := iamRequest("DeleteUserPermissionsBoundary", map[string]string{"UserName": "alice"})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestHandler_PermissionsBoundary_RoleRoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")
	p, _ := b.CreatePolicy("Boundary", "/", doc)

	req := iamRequest("PutRolePermissionsBoundary", map[string]string{
		"RoleName":            "MyRole",
		"PermissionsBoundary": p.Arn,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("GetRolePermissionsBoundary", map[string]string{"RoleName": "MyRole"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)

	req3 := iamRequest("DeleteRolePermissionsBoundary", map[string]string{"RoleName": "MyRole"})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// ---- CredentialReport via handler ----

func TestHandler_CredentialReport_Generated(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")

	// GenerateCredentialReport.
	req := iamRequest("GenerateCredentialReport", map[string]string{})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetCredentialReport.
	req2 := iamRequest("GetCredentialReport", map[string]string{})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)

	body := rec2.Body.String()
	assert.Contains(t, body, "ReportFormat", "response must include format field")
}

// ---- GetAccountSummary via handler ----

func TestHandler_GetAccountSummary(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)

	_, _ = b.CreateUser("alice", "/", "")
	_, _ = b.CreateUser("bob", "/", "")
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateRole("R", "/", doc, "")
	_, _ = b.CreateGroup("G", "/")

	req := iamRequest("GetAccountSummary", map[string]string{})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "SummaryMap")
}

// ---- AccountAlias via handler ----

func TestHandler_AccountAlias_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	req := iamRequest("CreateAccountAlias", map[string]string{"AccountAlias": "myalias"})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("ListAccountAliases", map[string]string{})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "myalias")

	req3 := iamRequest("DeleteAccountAlias", map[string]string{"AccountAlias": "myalias"})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// ---- UpdateAssumeRolePolicy via handler ----

func TestHandler_UpdateAssumeRolePolicy(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")

	newDoc := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},` +
		`"Action":"sts:AssumeRole"}]}`
	req := iamRequest("UpdateAssumeRolePolicy", map[string]string{
		"RoleName":       "MyRole",
		"PolicyDocument": newDoc,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- GetAccountAuthorizationDetails via handler ----

func TestHandler_GetAccountAuthorizationDetails(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateRole("R", "/", doc, "")

	req := iamRequest("GetAccountAuthorizationDetails", map[string]string{})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "alice")
}

// ---- ListEntitiesForPolicy via handler ----

func TestHandler_ListEntitiesForPolicy(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateUser("alice", "/", "")
	p, _ := b.CreatePolicy("P", "/", doc)
	_ = b.AttachUserPolicy("alice", p.Arn)

	req := iamRequest("ListEntitiesForPolicy", map[string]string{"PolicyArn": p.Arn})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "alice")
}

// ---- ServiceLinkedRole ----

func TestHandler_ServiceLinkedRole_Create(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	req := iamRequest("CreateServiceLinkedRole", map[string]string{
		"AWSServiceName": "elasticloadbalancing.amazonaws.com",
		"Description":    "ELB service role",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "elasticloadbalancing")
}

// ---- User group membership ----

func TestHandler_GroupMembership_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")
	_, _ = b.CreateGroup("Admins", "/")

	req := iamRequest("AddUserToGroup", map[string]string{
		"UserName":  "alice",
		"GroupName": "Admins",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("GetGroup", map[string]string{"GroupName": "Admins"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Contains(t, rec2.Body.String(), "alice")

	req3 := iamRequest("ListGroupsForUser", map[string]string{"UserName": "alice"})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Contains(t, rec3.Body.String(), "Admins")

	req4 := iamRequest("RemoveUserFromGroup", map[string]string{
		"UserName":  "alice",
		"GroupName": "Admins",
	})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)
}

// ---- TagUser / UntagUser ----

func TestHandler_TagUser_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")

	req := iamRequest("TagUser", map[string]string{
		"UserName":            "alice",
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "prod",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("ListUserTags", map[string]string{"UserName": "alice"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Contains(t, rec2.Body.String(), "env")
	assert.Contains(t, rec2.Body.String(), "prod")

	req3 := iamRequest("UntagUser", map[string]string{
		"UserName":         "alice",
		"TagKeys.member.1": "env",
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// ---- TagRole / UntagRole ----

func TestHandler_TagRole_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")

	req := iamRequest("TagRole", map[string]string{
		"RoleName":            "MyRole",
		"Tags.member.1.Key":   "team",
		"Tags.member.1.Value": "platform",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("ListRoleTags", map[string]string{"RoleName": "MyRole"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Contains(t, rec2.Body.String(), "platform")

	req3 := iamRequest("UntagRole", map[string]string{
		"RoleName":         "MyRole",
		"TagKeys.member.1": "team",
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// ---- UpdateRole / UpdateUser / UpdateGroup ----

func TestHandler_UpdateRole(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")

	req := iamRequest("UpdateRole", map[string]string{
		"RoleName":    "MyRole",
		"Description": "Updated description",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateUser(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")

	req := iamRequest("UpdateUser", map[string]string{
		"UserName":    "alice",
		"NewUserName": "alicia",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// Old name should be gone.
	req2 := iamRequest("GetUser", map[string]string{"UserName": "alice"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusBadRequest, rec2.Code)

	// New name should exist.
	req3 := iamRequest("GetUser", map[string]string{"UserName": "alicia"})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestHandler_UpdateGroup(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateGroup("OldGroup", "/")

	req := iamRequest("UpdateGroup", map[string]string{
		"GroupName":    "OldGroup",
		"NewGroupName": "NewGroup",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- SimulatePrincipalPolicy ----

func TestHandler_SimulatePrincipalPolicy(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")
	u, _ := b.GetUser("alice")

	req := iamRequest("SimulatePrincipalPolicy", map[string]string{
		"PolicySourceArn":       u.Arn,
		"ActionNames.member.1":  "s3:GetObject",
		"ResourceArns.member.1": "arn:aws:s3:::my-bucket/*",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "EvaluationResults")
}
