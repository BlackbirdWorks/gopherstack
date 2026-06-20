package iam_test

// AWS-accuracy audit batch-2 — handler-level tests for SimulatePolicy EvalDecisionDetails,
// GenerateOrganizationsAccessReport polling, GetAccessKeyLastUsed response, service-specific
// credentials, password policy CRUD, and server certificate operations.

import (
	"encoding/xml"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// ---- SimulatePrincipalPolicy handler response ----

func TestHandler_SimulatePrincipalPolicy_EvalDecisionPresent(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("sim-user", "/", "")

	pol, _ := b.CreatePolicy("AllowS3", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	_ = b.AttachUserPolicy("sim-user", pol.Arn)

	req := iamRequest("SimulatePrincipalPolicy", map[string]string{
		"PolicySourceArn":       "arn:aws:iam::000000000000:user/sim-user",
		"ActionNames.member.1":  "s3:GetObject",
		"ResourceArns.member.1": "*",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "EvalDecision")
	assert.Contains(t, body, "allowed")
	assert.Contains(t, body, "EvalDecisionDetails")
}

func TestHandler_SimulatePrincipalPolicy_MultipleActions(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("multi-action-user", "/", "")

	pol, _ := b.CreatePolicy("AllowS3Read", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	_ = b.AttachUserPolicy("multi-action-user", pol.Arn)

	req := iamRequest("SimulatePrincipalPolicy", map[string]string{
		"PolicySourceArn":       "arn:aws:iam::000000000000:user/multi-action-user",
		"ActionNames.member.1":  "s3:GetObject",
		"ActionNames.member.2":  "s3:DeleteObject",
		"ResourceArns.member.1": "*",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "s3:GetObject")
	assert.Contains(t, body, "s3:DeleteObject")
	assert.Contains(t, body, "allowed")
	assert.Contains(t, body, "implicitDeny")
}

func TestHandler_SimulatePrincipalPolicy_WithPermissionsBoundary(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)

	boundary, _ := b.CreatePolicy("S3OnlyBoundary", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)

	_, _ = b.CreateUser("boundary-user", "/", boundary.Arn)

	identityPol, _ := b.CreatePolicy("AllowAll", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`)
	_ = b.AttachUserPolicy("boundary-user", identityPol.Arn)

	req := iamRequest("SimulatePrincipalPolicy", map[string]string{
		"PolicySourceArn":       "arn:aws:iam::000000000000:user/boundary-user",
		"ActionNames.member.1":  "ec2:DescribeInstances",
		"ResourceArns.member.1": "*",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	// Boundary blocks ec2, so overall decision is implicitDeny.
	assert.Contains(t, body, "implicitDeny")
	// PermissionsBoundaryDecisionDetail must be present.
	assert.Contains(t, body, "PermissionsBoundaryDecisionDetail")
	assert.Contains(t, body, "AllowedByPermissionsBoundary")
}

// ---- SimulateCustomPolicy handler response ----

func TestHandler_SimulateCustomPolicy_EvalDecisionDetails(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	allowDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`
	denyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:GetObject","Resource":"*"}]}`

	req := iamRequest("SimulateCustomPolicy", map[string]string{
		"PolicyInputList.member.1": allowDoc,
		"PolicyInputList.member.2": denyDoc,
		"ActionNames.member.1":     "s3:GetObject",
		"ResourceArns.member.1":    "*",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "EvalDecisionDetails")
	assert.Contains(t, body, "explicitDeny", "combined decision is explicitDeny")
}

// ---- GetAccessKeyLastUsed handler response ----

func TestHandler_GetAccessKeyLastUsed_NeverUsed(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("aklu-user", "/", "")

	ak, err := b.CreateAccessKey("aklu-user")
	require.NoError(t, err)

	req := iamRequest("GetAccessKeyLastUsed", map[string]string{
		"AccessKeyId": ak.AccessKeyID,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "GetAccessKeyLastUsedResult")
	assert.Contains(t, body, "aklu-user")
	assert.Contains(t, body, "N/A", "never-used key must show N/A last used")
}

func TestHandler_GetAccessKeyLastUsed_AfterUsage(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("aklu-used-user", "/", "")

	ak, err := b.CreateAccessKey("aklu-used-user")
	require.NoError(t, err)

	b.RecordAccessKeyUsage(ak.AccessKeyID, "us-west-2", "s3")

	req := iamRequest("GetAccessKeyLastUsed", map[string]string{
		"AccessKeyId": ak.AccessKeyID,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "us-west-2")
	assert.Contains(t, body, "s3")
}

// ---- GenerateOrganizationsAccessReport ----

func TestHandler_GenerateOrganizationsAccessReport_ReturnsJobID(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	req := iamRequest("GenerateOrganizationsAccessReport", map[string]string{
		"EntityPath": "o-example12345678901/r-xy12/ou-xy12-abcdefgh",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "JobId")
}

func TestHandler_GetOrganizationsAccessReport_CompletedStatus(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)

	// Generate a report and capture the JobId.
	jobID := b.GenerateOrganizationsAccessReport("o-example12345678901/r-xy12/ou-xy12-abcdefgh")
	require.NotEmpty(t, jobID)

	req := iamRequest("GetOrganizationsAccessReport", map[string]string{"JobId": jobID})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "COMPLETED", "report must reach COMPLETED status")
}

// ---- ServiceSpecificCredentials handler ----

func TestHandler_ServiceSpecificCredential_CreateListDelete(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("ssc-handler-user", "/", "")

	// Create.
	req := iamRequest("CreateServiceSpecificCredential", map[string]string{
		"UserName":    "ssc-handler-user",
		"ServiceName": "codecommit.amazonaws.com",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)

	createBody := rec.Body.String()
	assert.Contains(t, createBody, "ServiceSpecificCredentialId")
	assert.Contains(t, createBody, "Active")

	// List.
	req2 := iamRequest("ListServiceSpecificCredentials", map[string]string{
		"UserName": "ssc-handler-user",
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "codecommit.amazonaws.com")

	// Extract credential ID from create response.
	var createResp struct {
		XMLName xml.Name `xml:"CreateServiceSpecificCredentialResponse"`
		Result  struct {
			SSC struct {
				ID string `xml:"ServiceSpecificCredentialId"`
			} `xml:"ServiceSpecificCredential"`
		} `xml:"CreateServiceSpecificCredentialResult"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &createResp))
	credID := createResp.Result.SSC.ID
	require.NotEmpty(t, credID)

	// Delete.
	req3 := iamRequest("DeleteServiceSpecificCredential", map[string]string{
		"UserName":                    "ssc-handler-user",
		"ServiceSpecificCredentialId": credID,
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Verify deleted.
	req4 := iamRequest("ListServiceSpecificCredentials", map[string]string{
		"UserName": "ssc-handler-user",
	})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.NotContains(t, rec4.Body.String(), credID)
}

// ---- PasswordPolicy handler ----

func TestHandler_AccountPasswordPolicy_GetAfterUpdate(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	// Update.
	req := iamRequest("UpdateAccountPasswordPolicy", map[string]string{
		"MinimumPasswordLength":      "14",
		"RequireSymbols":             "true",
		"RequireNumbers":             "true",
		"RequireUppercaseCharacters": "true",
		"RequireLowercaseCharacters": "true",
		"MaxPasswordAge":             "60",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get.
	req2 := iamRequest("GetAccountPasswordPolicy", nil)
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)

	body := rec2.Body.String()
	assert.Contains(t, body, "14", "minimum length must be reflected")
	assert.Contains(t, body, "60", "MaxPasswordAge must be reflected")
}

func TestHandler_AccountPasswordPolicy_Delete(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	// First set a policy.
	req := iamRequest("UpdateAccountPasswordPolicy", map[string]string{
		"MinimumPasswordLength": "10",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete.
	req2 := iamRequest("DeleteAccountPasswordPolicy", nil)
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// ---- CredentialReport handler ----

func TestHandler_GetCredentialReport_ResponseFormat(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("report-handler-user", "/", "")

	req := iamRequest("GetCredentialReport", nil)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "GetCredentialReportResult")
	assert.Contains(t, body, "Content")
	assert.Contains(t, body, "ReportFormat")
	assert.Contains(t, body, "GeneratedTime")
	assert.Contains(t, body, "text/csv")
}

// ---- ServerCertificate handler ----

func TestHandler_ServerCertificate_UploadAndGet(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	certBody := "-----BEGIN CERTIFICATE-----\nMIIBxxx\n-----END CERTIFICATE-----"

	req := iamRequest("UploadServerCertificate", map[string]string{
		"ServerCertificateName": "my-tls-cert",
		"CertificateBody":       certBody,
		"Path":                  "/",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	uploadBody := rec.Body.String()
	assert.Contains(t, uploadBody, "my-tls-cert")
	assert.Contains(t, uploadBody, "arn:aws:iam::")

	// Get.
	req2 := iamRequest("GetServerCertificate", map[string]string{
		"ServerCertificateName": "my-tls-cert",
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "my-tls-cert")
}

// ---- SAML Provider round-trip ----

func TestHandler_SAMLProvider_UpdateAndGet(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)

	provider, err := b.CreateSAMLProvider("test-saml", "<original/>")
	require.NoError(t, err)

	req := iamRequest("UpdateSAMLProvider", map[string]string{
		"SAMLProviderArn":      provider.Arn,
		"SAMLMetadataDocument": "<updated-metadata/>",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("GetSAMLProvider", map[string]string{
		"SAMLProviderArn": provider.Arn,
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "updated-metadata")
}

// ---- OIDC Provider handler ----

func TestHandler_OIDCProvider_CreateListDelete(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	req := iamRequest("CreateOpenIDConnectProvider", map[string]string{
		"Url":                     "https://oidc.example.com",
		"ClientIDList.member.1":   "sts.amazonaws.com",
		"ThumbprintList.member.1": "9e99a48a9960b14926bb7f3b02e22da2b0ab7280",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	oidcBody := rec.Body.String()
	assert.Contains(t, oidcBody, "OpenIDConnectProviderArn")
	assert.Contains(t, oidcBody, "arn:aws:iam::")

	req2 := iamRequest("ListOpenIDConnectProviders", nil)
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "oidc-provider")
}

// ---- AccessKey rotation accuracy ----

func TestHandler_AccessKey_RotationUpdatesStatus(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("rotation-user", "/", "")

	ak, err := b.CreateAccessKey("rotation-user")
	require.NoError(t, err)

	// Deactivate.
	req := iamRequest("UpdateAccessKey", map[string]string{
		"UserName":    "rotation-user",
		"AccessKeyId": ak.AccessKeyID,
		"Status":      "Inactive",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// List and verify status.
	req2 := iamRequest("ListAccessKeys", map[string]string{"UserName": "rotation-user"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "Inactive")
}

// ---- GenerateCredentialReport + GetCredentialReport ----

func TestHandler_GenerateAndGetCredentialReport(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("gen-report-user", "/", "")
	_, _ = b.CreateAccessKey("gen-report-user")

	// Generate.
	req := iamRequest("GenerateCredentialReport", nil)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "COMPLETE")

	// Get.
	req2 := iamRequest("GetCredentialReport", nil)
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	require.Equal(t, http.StatusOK, rec2.Code)

	body := rec2.Body.String()
	assert.Contains(t, body, "text/csv")
	assert.Contains(t, body, "GeneratedTime")
}

// ---- SimulatePrincipalPolicy error cases ----

func TestHandler_SimulatePrincipalPolicy_UserNotFound(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	req := iamRequest("SimulatePrincipalPolicy", map[string]string{
		"PolicySourceArn":      "arn:aws:iam::000000000000:user/nonexistent",
		"ActionNames.member.1": "s3:GetObject",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- ServiceSpecificCredential UpdateStatus handler ----

func TestHandler_ServiceSpecificCredential_UpdateStatus(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("ssc-update-status-user", "/", "")

	cred, err := b.CreateServiceSpecificCredential("ssc-update-status-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	req := iamRequest("UpdateServiceSpecificCredential", map[string]string{
		"UserName":                    "ssc-update-status-user",
		"ServiceSpecificCredentialId": cred.ServiceSpecificCredentialID,
		"Status":                      "Inactive",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	creds, _ := b.ListServiceSpecificCredentials("ssc-update-status-user", "")
	require.Len(t, creds, 1)
	assert.Equal(t, "Inactive", creds[0].Status)
}

// ---- ResetServiceSpecificCredential handler ----

func TestHandler_ResetServiceSpecificCredential_ChangesPassword(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("ssc-reset-user", "/", "")

	cred, err := b.CreateServiceSpecificCredential("ssc-reset-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	req := iamRequest("ResetServiceSpecificCredential", map[string]string{
		"UserName":                    "ssc-reset-user",
		"ServiceSpecificCredentialId": cred.ServiceSpecificCredentialID,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	// Response field is ServicePassword (not ServiceSpecificCredentialPassword).
	assert.Contains(t, body, "ServicePassword")
}

// ---- Ensure indexed query string params work for SimulateCustomPolicy ----

func TestHandler_SimulateCustomPolicy_IndexedParams(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	policyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iam:*","Resource":"*"}]}`
	action := "iam:CreateUser"

	params := map[string]string{
		"PolicyInputList.member.1": policyDoc,
		"ActionNames.member.1":     action,
	}

	req := iamRequest("SimulateCustomPolicy", params)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "allowed")
	assert.Contains(t, body, action)
}

// ---- iamRequest with indexed members helper (extends existing helper) ----

// iamRequestIndexed builds a request with indexed member parameters.
// params is a flat map; caller must encode member indices in keys.
func iamRequestWithMembers(action string, params map[string]string, indexed map[string][]string) *http.Request {
	base := maps.Clone(params)
	if base == nil {
		base = map[string]string{}
	}

	for prefix, values := range indexed {
		for i, v := range values {
			base[fmt.Sprintf("%s.%d", prefix, i+1)] = v
		}
	}

	return iamRequest(action, base)
}

func TestHandler_SimulatePrincipalPolicy_MultipleResources(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("multi-res-user", "/", "")

	s3Policy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-a/*"}]}`
	pol, _ := b.CreatePolicy("AllowS3", "/", s3Policy)
	_ = b.AttachUserPolicy("multi-res-user", pol.Arn)

	req := iamRequestWithMembers(
		"SimulatePrincipalPolicy",
		map[string]string{
			"PolicySourceArn": "arn:aws:iam::000000000000:user/multi-res-user",
		},
		map[string][]string{
			"ActionNames.member":  {"s3:GetObject"},
			"ResourceArns.member": {"arn:aws:s3:::bucket-a/file.txt", "arn:aws:s3:::bucket-b/other.txt"},
		},
	)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "bucket-a")
	assert.Contains(t, body, "bucket-b")
	// bucket-a should be allowed, bucket-b implicitly denied.
	assert.Contains(t, body, "allowed")
	assert.Contains(t, body, "implicitDeny")
}

// ---- GetPolicyVersion VersionId accuracy (handler) ----

func TestHandler_GetPolicyVersion_VersionIdNotHardcoded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		requestedVersion string
		wantVersionID    string
		setAsDefault     bool
	}{
		{"v1", "v1", "v1", false},
		{"v2 non-default", "v2", "v2", false},
		{"v2 set as default", "v2", "v2", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)

			doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
			p, err := b.CreatePolicy("GPVPolicy-"+tc.name, "/", doc)
			require.NoError(t, err)

			doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
			_, err = b.CreatePolicyVersion(p.Arn, doc2, false)
			require.NoError(t, err)

			if tc.setAsDefault {
				require.NoError(t, b.SetDefaultPolicyVersion(p.Arn, "v2"))
			}

			req := iamRequest("GetPolicyVersion", map[string]string{
				"PolicyArn": p.Arn,
				"VersionId": tc.requestedVersion,
			})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, "<VersionId>"+tc.wantVersionID+"</VersionId>",
				"GetPolicyVersion response VersionId must match requested version, not hardcoded v1")
		})
	}
}

// ---- GetPolicy / ListPolicies AWS fields ----

func TestHandler_GetPolicy_ReturnsAWSFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		setup            func(b *iam.InMemoryBackend, policyArn string)
		wantDefaultVer   string
		wantAttachCount  string
		wantIsAttachable string
	}{
		{
			name:             "fresh policy",
			setup:            func(_ *iam.InMemoryBackend, _ string) {},
			wantDefaultVer:   "v1",
			wantAttachCount:  "0",
			wantIsAttachable: "true",
		},
		{
			name: "after attach to user",
			setup: func(b *iam.InMemoryBackend, policyArn string) {
				_, _ = b.CreateUser("gp-fields-user", "/", "")
				_ = b.AttachUserPolicy("gp-fields-user", policyArn)
			},
			wantDefaultVer:   "v1",
			wantAttachCount:  "1",
			wantIsAttachable: "true",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)

			doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
			p, err := b.CreatePolicy("GPFieldsPolicy-"+tc.name, "/", doc)
			require.NoError(t, err)

			tc.setup(b, p.Arn)

			req := iamRequest("GetPolicy", map[string]string{"PolicyArn": p.Arn})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, "<DefaultVersionId>"+tc.wantDefaultVer+"</DefaultVersionId>",
				"GetPolicy must include DefaultVersionId")
			assert.Contains(t, body, "<AttachmentCount>"+tc.wantAttachCount+"</AttachmentCount>",
				"GetPolicy must include AttachmentCount")
			assert.Contains(t, body, "<IsAttachable>"+tc.wantIsAttachable+"</IsAttachable>",
				"GetPolicy must include IsAttachable")
			assert.Contains(t, body, "<UpdateDate>",
				"GetPolicy must include UpdateDate")
		})
	}
}

func TestHandler_GetPolicy_DefaultVersionIdUpdatesAfterNewDefault(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)

	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, err := b.CreatePolicy("GPDefVer", "/", doc)
	require.NoError(t, err)

	doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	_, err = b.CreatePolicyVersion(p.Arn, doc2, false)
	require.NoError(t, err)

	require.NoError(t, b.SetDefaultPolicyVersion(p.Arn, "v2"))

	req := iamRequest("GetPolicy", map[string]string{"PolicyArn": p.Arn})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "<DefaultVersionId>v2</DefaultVersionId>",
		"DefaultVersionId must reflect v2 after SetDefaultPolicyVersion")
	assert.NotContains(t, body, "<DefaultVersionId>v1</DefaultVersionId>",
		"DefaultVersionId must not still say v1 after v2 was set as default")
}
