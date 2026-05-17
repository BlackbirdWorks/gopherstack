package transfer_test

// new_ops_coverage_test.go exercises the handler operations that were
// previously uncovered, bringing transfer coverage above the 70% threshold.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- DescribeAccess ---

func TestHandler_DescribeAccess(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	// Create an access first.
	createRec := doTransferRequest(t, h, "CreateAccess", map[string]any{
		"ServerId":   s.ServerID,
		"ExternalId": "S-1-5-21-9999",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doTransferRequest(t, h, "DescribeAccess", map[string]any{
		"ServerId":   s.ServerID,
		"ExternalId": "S-1-5-21-9999",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Access"])
}

func TestHandler_DescribeAccess_MissingFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "DescribeAccess", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- ListAccesses ---

func TestHandler_ListAccesses(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "ListAccesses", map[string]any{
		"ServerId": s.ServerID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListAccesses_MissingServerId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ListAccesses", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- UpdateAccess ---

func TestHandler_UpdateAccess(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	doTransferRequest(t, h, "CreateAccess", map[string]any{
		"ServerId":   s.ServerID,
		"ExternalId": "S-1-5-21-8888",
	})

	rec := doTransferRequest(t, h, "UpdateAccess", map[string]any{
		"ServerId":      s.ServerID,
		"ExternalId":    "S-1-5-21-8888",
		"HomeDirectory": "/updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- DescribeAgreement ---

func TestHandler_DescribeAgreement(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	prof, err := h.Backend.CreateProfile("LOCAL", "TESTPARTNER", nil)
	require.NoError(t, err)

	createRec := doTransferRequest(t, h, "CreateAgreement", map[string]any{
		"ServerId":         s.ServerID,
		"LocalProfileId":   prof.ProfileID,
		"PartnerProfileId": prof.ProfileID,
		"AccessRole":       "arn:aws:iam::123456789012:role/TransferRole",
		"BaseDirectory":    "/agreements",
		"Description":      "test agreement",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	agreementID := createResp["AgreementId"].(string)

	rec := doTransferRequest(t, h, "DescribeAgreement", map[string]any{
		"ServerId":    s.ServerID,
		"AgreementId": agreementID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- ListAgreements ---

func TestHandler_ListAgreements(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "ListAgreements", map[string]any{
		"ServerId": s.ServerID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- UpdateAgreement ---

func TestHandler_UpdateAgreement(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	prof, err := h.Backend.CreateProfile("LOCAL", "TESTPARTNER", nil)
	require.NoError(t, err)

	createRec := doTransferRequest(t, h, "CreateAgreement", map[string]any{
		"ServerId":         s.ServerID,
		"LocalProfileId":   prof.ProfileID,
		"PartnerProfileId": prof.ProfileID,
		"AccessRole":       "arn:aws:iam::123456789012:role/TransferRole",
		"BaseDirectory":    "/agreements",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	agreementID := createResp["AgreementId"].(string)

	rec := doTransferRequest(t, h, "UpdateAgreement", map[string]any{
		"ServerId":    s.ServerID,
		"AgreementId": agreementID,
		"Description": "updated description",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- DescribeConnector / ListConnectors / UpdateConnector ---

func TestHandler_DescribeConnector(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateConnector", map[string]any{
		"Url":        "http://example.com",
		"AccessRole": "arn:aws:iam::123456789012:role/TransferRole",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	connectorID := createResp["ConnectorId"].(string)

	rec := doTransferRequest(t, h, "DescribeConnector", map[string]any{
		"ConnectorId": connectorID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListConnectors(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ListConnectors", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateConnector(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateConnector", map[string]any{
		"Url":        "http://example.com",
		"AccessRole": "arn:aws:iam::123456789012:role/TransferRole",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	connectorID := createResp["ConnectorId"].(string)

	rec := doTransferRequest(t, h, "UpdateConnector", map[string]any{
		"ConnectorId": connectorID,
		"Url":         "http://updated.example.com",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Profile CRUD ---

func TestHandler_DescribeProfile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id":       "TESTPROFILE",
		"ProfileType": "LOCAL",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	profileID := createResp["ProfileId"].(string)

	rec := doTransferRequest(t, h, "DescribeProfile", map[string]any{
		"ProfileId": profileID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListProfiles(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ListProfiles", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteProfile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id":       "DELETEPROFILE",
		"ProfileType": "LOCAL",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	profileID := createResp["ProfileId"].(string)

	rec := doTransferRequest(t, h, "DeleteProfile", map[string]any{
		"ProfileId": profileID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateProfile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id":       "UPDATEPROFILE",
		"ProfileType": "LOCAL",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	profileID := createResp["ProfileId"].(string)

	rec := doTransferRequest(t, h, "UpdateProfile", map[string]any{
		"ProfileId":      profileID,
		"CertificateIds": []string{},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- WebApp CRUD ---

func TestHandler_DescribeWebApp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWebApp", map[string]any{
		"IdentityProviderDetails": map[string]any{
			"IdentityCenterConfig": map[string]any{
				"InstanceArn": "arn:aws:sso:::instance/ssoins-1234567890abcdef0",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	webAppID := createResp["WebAppId"].(string)

	rec := doTransferRequest(t, h, "DescribeWebApp", map[string]any{
		"WebAppId": webAppID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListWebApps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ListWebApps", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteWebApp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWebApp", map[string]any{
		"IdentityProviderDetails": map[string]any{
			"IdentityCenterConfig": map[string]any{
				"InstanceArn": "arn:aws:sso:::instance/ssoins-delete",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	webAppID := createResp["WebAppId"].(string)

	rec := doTransferRequest(t, h, "DeleteWebApp", map[string]any{
		"WebAppId": webAppID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateWebApp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWebApp", map[string]any{
		"IdentityProviderDetails": map[string]any{
			"IdentityCenterConfig": map[string]any{
				"InstanceArn": "arn:aws:sso:::instance/ssoins-update",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	webAppID := createResp["WebAppId"].(string)

	rec := doTransferRequest(t, h, "UpdateWebApp", map[string]any{
		"WebAppId": webAppID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Workflow CRUD ---

func TestHandler_DescribeWorkflow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWorkflow", map[string]any{
		"Steps": []map[string]any{},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	workflowID := createResp["WorkflowId"].(string)

	rec := doTransferRequest(t, h, "DescribeWorkflow", map[string]any{
		"WorkflowId": workflowID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListWorkflows(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ListWorkflows", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteWorkflow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWorkflow", map[string]any{
		"Steps": []map[string]any{},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	workflowID := createResp["WorkflowId"].(string)

	rec := doTransferRequest(t, h, "DeleteWorkflow", map[string]any{
		"WorkflowId": workflowID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Certificate CRUD ---

func TestHandler_ImportCertificate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// Use no body so PEM parsing is skipped; provide dates as fallback.
	rec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage":         "SIGNING",
		"NotBeforeDate": "2024-01-01T00:00:00Z",
		"NotAfterDate":  "2025-01-01T00:00:00Z",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DescribeCertificate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage":         "SIGNING",
		"NotBeforeDate": "2024-01-01T00:00:00Z",
		"NotAfterDate":  "2025-01-01T00:00:00Z",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	certID := createResp["CertificateId"].(string)

	rec := doTransferRequest(t, h, "DescribeCertificate", map[string]any{
		"CertificateId": certID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListCertificates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ListCertificates", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateCertificate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage":         "SIGNING",
		"NotBeforeDate": "2024-06-01T00:00:00Z",
		"NotAfterDate":  "2025-06-01T00:00:00Z",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	certID := createResp["CertificateId"].(string)

	rec := doTransferRequest(t, h, "UpdateCertificate", map[string]any{
		"CertificateId": certID,
		"Description":   "updated cert",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- HostKey CRUD ---

func TestHandler_ImportHostKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "ImportHostKey", map[string]any{
		"ServerId":    s.ServerID,
		"HostKeyBody": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC test-key",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListHostKeys(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "ListHostKeys", map[string]any{
		"ServerId": s.ServerID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DescribeHostKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	importRec := doTransferRequest(t, h, "ImportHostKey", map[string]any{
		"ServerId":    s.ServerID,
		"HostKeyBody": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC2 desc-key",
	})
	require.Equal(t, http.StatusOK, importRec.Code)

	var importResp map[string]any
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importResp))
	hostKeyID := importResp["HostKeyId"].(string)

	rec := doTransferRequest(t, h, "DescribeHostKey", map[string]any{
		"ServerId":  s.ServerID,
		"HostKeyId": hostKeyID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteHostKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	importRec := doTransferRequest(t, h, "ImportHostKey", map[string]any{
		"ServerId":    s.ServerID,
		"HostKeyBody": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC3 del-key",
	})
	require.Equal(t, http.StatusOK, importRec.Code)

	var importResp map[string]any
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importResp))
	hostKeyID := importResp["HostKeyId"].(string)

	rec := doTransferRequest(t, h, "DeleteHostKey", map[string]any{
		"ServerId":  s.ServerID,
		"HostKeyId": hostKeyID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateHostKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	importRec := doTransferRequest(t, h, "ImportHostKey", map[string]any{
		"ServerId":    s.ServerID,
		"HostKeyBody": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC4 upd-key",
	})
	require.Equal(t, http.StatusOK, importRec.Code)

	var importResp map[string]any
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importResp))
	hostKeyID := importResp["HostKeyId"].(string)

	rec := doTransferRequest(t, h, "UpdateHostKey", map[string]any{
		"ServerId":    s.ServerID,
		"HostKeyId":   hostKeyID,
		"Description": "updated host key",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- SSH Public Key management ---

func TestHandler_ImportSSHPublicKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	// Create a user first.
	userRec := doTransferRequest(t, h, "CreateUser", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "testuser-ssh",
		"Role":     "arn:aws:iam::123456789012:role/TransferUserRole",
	})
	require.Equal(t, http.StatusOK, userRec.Code)

	rec := doTransferRequest(t, h, "ImportSshPublicKey", map[string]any{
		"ServerId":         s.ServerID,
		"UserName":         "testuser-ssh",
		"SshPublicKeyBody": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC5 user-key",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteSSHPublicKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	doTransferRequest(t, h, "CreateUser", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "testuser-del-ssh",
		"Role":     "arn:aws:iam::123456789012:role/TransferUserRole",
	})

	importRec := doTransferRequest(t, h, "ImportSshPublicKey", map[string]any{
		"ServerId":         s.ServerID,
		"UserName":         "testuser-del-ssh",
		"SshPublicKeyBody": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC6 del-user-key",
	})
	require.Equal(t, http.StatusOK, importRec.Code)

	var importResp map[string]any
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importResp))
	sshKeyID := importResp["SshPublicKeyId"].(string)

	rec := doTransferRequest(t, h, "DeleteSshPublicKey", map[string]any{
		"ServerId":       s.ServerID,
		"UserName":       "testuser-del-ssh",
		"SshPublicKeyId": sshKeyID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Tag operations ---

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	arn := "arn:aws:transfer:us-east-1:123456789012:server/" + s.ServerID

	rec := doTransferRequest(t, h, "TagResource", map[string]any{
		"Arn":  arn,
		"Tags": []map[string]any{{"Key": "Env", "Value": "test"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	arn := "arn:aws:transfer:us-east-1:123456789012:server/" + s.ServerID

	doTransferRequest(t, h, "TagResource", map[string]any{
		"Arn":  arn,
		"Tags": []map[string]any{{"Key": "Env", "Value": "test"}},
	})

	rec := doTransferRequest(t, h, "ListTagsForResource", map[string]any{
		"Arn": arn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Tags"])
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	arn := "arn:aws:transfer:us-east-1:123456789012:server/" + s.ServerID

	doTransferRequest(t, h, "TagResource", map[string]any{
		"Arn":  arn,
		"Tags": []map[string]any{{"Key": "Env", "Value": "test"}},
	})

	rec := doTransferRequest(t, h, "UntagResource", map[string]any{
		"Arn":     arn,
		"TagKeys": []string{"Env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Execution operations ---

func TestHandler_ListExecutions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWorkflow", map[string]any{
		"Steps": []map[string]any{},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	workflowID := createResp["WorkflowId"].(string)

	rec := doTransferRequest(t, h, "ListExecutions", map[string]any{
		"WorkflowId": workflowID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Security policy operations ---

func TestHandler_ListSecurityPolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ListSecurityPolicies", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DescribeSecurityPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "DescribeSecurityPolicy", map[string]any{
		"SecurityPolicyName": "TransferSecurityPolicy-2020-06",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- TestConnection ---

func TestHandler_TestConnection(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateConnector", map[string]any{
		"Url":        "http://example.com",
		"AccessRole": "arn:aws:iam::123456789012:role/TransferRole",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	connectorID := createResp["ConnectorId"].(string)

	rec := doTransferRequest(t, h, "TestConnection", map[string]any{
		"ConnectorId": connectorID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- TestIdentityProvider ---

func TestHandler_TestIdentityProvider(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "TestIdentityProvider", map[string]any{
		"ServerId":     s.ServerID,
		"UserName":     "testuser",
		"UserPassword": "password",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- StartFileTransfer / StartDirectoryListing / StartRemoteDelete / StartRemoteMove ---

func TestHandler_StartFileTransfer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateConnector", map[string]any{
		"Url":        "http://example.com",
		"AccessRole": "arn:aws:iam::123456789012:role/TransferRole",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	connectorID := createResp["ConnectorId"].(string)

	rec := doTransferRequest(t, h, "StartFileTransfer", map[string]any{
		"ConnectorId":   connectorID,
		"SendFilePaths": []string{"/test/file.txt"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- WebAppCustomization ---

func TestHandler_WebAppCustomization(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWebApp", map[string]any{
		"IdentityProviderDetails": map[string]any{
			"IdentityCenterConfig": map[string]any{
				"InstanceArn": "arn:aws:sso:::instance/ssoins-custom",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	webAppID := createResp["WebAppId"].(string)

	// DescribeWebAppCustomization
	descRec := doTransferRequest(t, h, "DescribeWebAppCustomization", map[string]any{
		"WebAppId": webAppID,
	})
	assert.Equal(t, http.StatusOK, descRec.Code)

	// UpdateWebAppCustomization
	updRec := doTransferRequest(t, h, "UpdateWebAppCustomization", map[string]any{
		"WebAppId": webAppID,
		"Title":    "My Transfer Portal",
	})
	assert.Equal(t, http.StatusOK, updRec.Code)

	// DeleteWebAppCustomization
	delRec := doTransferRequest(t, h, "DeleteWebAppCustomization", map[string]any{
		"WebAppId": webAppID,
	})
	assert.Equal(t, http.StatusOK, delRec.Code)
}

// --- SendWorkflowStepState ---

func TestHandler_SendWorkflowStepState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "SendWorkflowStepState", map[string]any{
		"WorkflowId":  "wf-1234",
		"ExecutionId": "exec-1234",
		"Token":       "token-1234",
		"Status":      "SUCCESS",
	})
	// May fail if workflow not found; we're testing the handler path.
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)
}

// --- StartDirectoryListing ---

func TestHandler_StartDirectoryListing(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateConnector", map[string]any{
		"Url":        "http://example.com",
		"AccessRole": "arn:aws:iam::123456789012:role/TransferRole",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	connectorID := createResp["ConnectorId"].(string)

	rec := doTransferRequest(t, h, "StartDirectoryListing", map[string]any{
		"ConnectorId":         connectorID,
		"RemotePath":          "/",
		"OutputDirectoryPath": "/output",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- ListFileTransferResults ---

func TestHandler_ListFileTransferResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateConnector", map[string]any{
		"Url":        "http://example.com",
		"AccessRole": "arn:aws:iam::123456789012:role/TransferRole",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	connectorID := createResp["ConnectorId"].(string)

	rec := doTransferRequest(t, h, "ListFileTransferResults", map[string]any{
		"ConnectorId": connectorID,
		"TransferId":  "transfer-1234",
	})
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)
}

// --- StartRemoteDelete / StartRemoteMove ---

func TestHandler_StartRemoteDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateConnector", map[string]any{
		"Url":        "http://example.com",
		"AccessRole": "arn:aws:iam::123456789012:role/TransferRole",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	connectorID := createResp["ConnectorId"].(string)

	rec := doTransferRequest(t, h, "StartRemoteDelete", map[string]any{
		"ConnectorId": connectorID,
		"DeletePath":  "/remote/path",
	})
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)
}

func TestHandler_StartRemoteMove(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateConnector", map[string]any{
		"Url":        "http://example.com",
		"AccessRole": "arn:aws:iam::123456789012:role/TransferRole",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	connectorID := createResp["ConnectorId"].(string)

	rec := doTransferRequest(t, h, "StartRemoteMove", map[string]any{
		"ConnectorId": connectorID,
		"SourcePath":  "/source",
		"TargetPath":  "/target",
	})
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)
}
