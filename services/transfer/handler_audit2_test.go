package transfer_test

// handler_audit2_test.go — AWS-accuracy audit batch-2 for Transfer Family (go-8z3j).
//
// Covers:
//   - DescribeProfile/ListProfiles include Arn and Tags
//   - DescribeWorkflow/ListWorkflows include Arn and Tags
//   - DescribeCertificate/ListCertificates include Arn
//   - DescribeAgreement includes Arn and Tags
//   - DescribeHostKey/ListHostKeys include HostKeyFingerprint, DateImported, Arn, Tags
//   - ImportCertificate rejects invalid Usage values
//   - HostKey fingerprint is computed on import

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Profile ARN + Tags
// ---------------------------------------------------------------------------

func TestAudit2_DescribeProfile_IncludesArnAndTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id":       "AS2-partner-1",
		"ProfileType": "LOCAL",
		"Tags":        []map[string]string{{"Key": "env", "Value": "test"}},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	profileID := createResp["ProfileId"].(string)

	rec := doTransferRequest(t, h, "DescribeProfile", map[string]any{
		"ProfileId": profileID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	profile := resp["Profile"].(map[string]any)

	arn, hasArn := profile["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in DescribeProfile response")
	assert.Contains(t, arn, profileID, "Arn must contain ProfileId")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")

	tags, hasTags := profile["Tags"].([]any)
	assert.True(t, hasTags, "Tags must be present in DescribeProfile response")
	assert.Len(t, tags, 1)
}

func TestAudit2_ListProfiles_IncludesArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id":       "AS2-partner-2",
		"ProfileType": "PARTNER",
	})

	rec := doTransferRequest(t, h, "ListProfiles", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	profiles := resp["Profiles"].([]any)
	require.NotEmpty(t, profiles)

	item := profiles[0].(map[string]any)
	arn, hasArn := item["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in ListProfiles items")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")
}

// ---------------------------------------------------------------------------
// Workflow ARN + Tags
// ---------------------------------------------------------------------------

func TestAudit2_DescribeWorkflow_IncludesArnAndTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWorkflow", map[string]any{
		"Description": "test workflow",
		"Tags":        []map[string]string{{"Key": "stage", "Value": "prod"}},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	workflowID := createResp["WorkflowId"].(string)

	rec := doTransferRequest(t, h, "DescribeWorkflow", map[string]any{
		"WorkflowId": workflowID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wf := resp["Workflow"].(map[string]any)

	arn, hasArn := wf["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in DescribeWorkflow response")
	assert.Contains(t, arn, workflowID, "Arn must contain WorkflowId")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")

	tags, hasTags := wf["Tags"].([]any)
	assert.True(t, hasTags, "Tags must be present in DescribeWorkflow response")
	assert.Len(t, tags, 1)
}

func TestAudit2_ListWorkflows_IncludesArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doTransferRequest(t, h, "CreateWorkflow", map[string]any{
		"Description": "wf for list test",
	})

	rec := doTransferRequest(t, h, "ListWorkflows", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	workflows := resp["Workflows"].([]any)
	require.NotEmpty(t, workflows)

	item := workflows[0].(map[string]any)
	arn, hasArn := item["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in ListWorkflows items")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")
}

// ---------------------------------------------------------------------------
// Certificate ARN
// ---------------------------------------------------------------------------

func TestAudit2_DescribeCertificate_IncludesArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage": "SIGNING",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	certID := createResp["CertificateId"].(string)

	rec := doTransferRequest(t, h, "DescribeCertificate", map[string]any{
		"CertificateId": certID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	cert := resp["Certificate"].(map[string]any)

	arn, hasArn := cert["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in DescribeCertificate response")
	assert.Contains(t, arn, certID, "Arn must contain CertificateId")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")
}

func TestAudit2_ListCertificates_IncludesArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage": "ENCRYPTION",
	})

	rec := doTransferRequest(t, h, "ListCertificates", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	certs := resp["Certificates"].([]any)
	require.NotEmpty(t, certs)

	item := certs[0].(map[string]any)
	arn, hasArn := item["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in ListCertificates items")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")
}

// ---------------------------------------------------------------------------
// Certificate Usage validation
// ---------------------------------------------------------------------------

func TestAudit2_ImportCertificate_UsageValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		usage    string
		wantCode int
	}{
		{"SIGNING accepted", "SIGNING", http.StatusOK},
		{"ENCRYPTION accepted", "ENCRYPTION", http.StatusOK},
		{"empty usage rejected", "", http.StatusBadRequest},
		{"invalid usage rejected", "INVALID", http.StatusBadRequest},
		{"lowercase rejected", "signing", http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{}
			if tc.usage != "" {
				body["Usage"] = tc.usage
			}
			rec := doTransferRequest(t, h, "ImportCertificate", body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// Agreement ARN + Tags
// ---------------------------------------------------------------------------

func TestAudit2_DescribeAgreement_IncludesArnAndTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Need two profiles and a server for an agreement.
	localRec := doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id": "local-as2", "ProfileType": "LOCAL",
	})
	require.Equal(t, http.StatusOK, localRec.Code)
	var localResp map[string]any
	require.NoError(t, json.Unmarshal(localRec.Body.Bytes(), &localResp))
	localProfileID := localResp["ProfileId"].(string)

	partnerRec := doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id": "partner-as2", "ProfileType": "PARTNER",
	})
	require.Equal(t, http.StatusOK, partnerRec.Code)
	var partnerResp map[string]any
	require.NoError(t, json.Unmarshal(partnerRec.Body.Bytes(), &partnerResp))
	partnerProfileID := partnerResp["ProfileId"].(string)

	serverRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, serverRec.Code)
	var serverResp map[string]any
	require.NoError(t, json.Unmarshal(serverRec.Body.Bytes(), &serverResp))
	serverID := serverResp["ServerId"].(string)

	agRec := doTransferRequest(t, h, "CreateAgreement", map[string]any{
		"ServerId":         serverID,
		"LocalProfileId":   localProfileID,
		"PartnerProfileId": partnerProfileID,
		"BaseDirectory":    "/transfers",
		"AccessRole":       "arn:aws:iam::000000000000:role/transfer-role",
		"Tags":             []map[string]string{{"Key": "team", "Value": "ops"}},
	})
	require.Equal(t, http.StatusOK, agRec.Code)
	var agResp map[string]any
	require.NoError(t, json.Unmarshal(agRec.Body.Bytes(), &agResp))
	agreementID := agResp["AgreementId"].(string)

	rec := doTransferRequest(t, h, "DescribeAgreement", map[string]any{
		"ServerId":    serverID,
		"AgreementId": agreementID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ag := resp["Agreement"].(map[string]any)

	arn, hasArn := ag["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in DescribeAgreement response")
	assert.Contains(t, arn, agreementID, "Arn must contain AgreementId")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")

	tags, hasTags := ag["Tags"].([]any)
	assert.True(t, hasTags, "Tags must be present in DescribeAgreement response")
	assert.Len(t, tags, 1)
}

// ---------------------------------------------------------------------------
// HostKey fingerprint, DateImported, Arn, Tags
// ---------------------------------------------------------------------------

const testHostKeyEd25519 = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GkZS test@host"

func TestAudit2_DescribeHostKey_IncludesFingerprint(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	serverRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, serverRec.Code)
	var serverResp map[string]any
	require.NoError(t, json.Unmarshal(serverRec.Body.Bytes(), &serverResp))
	serverID := serverResp["ServerId"].(string)

	importRec := doTransferRequest(t, h, "ImportHostKey", map[string]any{
		"ServerId":    serverID,
		"HostKeyBody": testHostKeyEd25519,
		"Description": "audit2 host key",
		"Tags":        []map[string]string{{"Key": "purpose", "Value": "test"}},
	})
	require.Equal(t, http.StatusOK, importRec.Code)
	var importResp map[string]any
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importResp))
	hostKeyID := importResp["HostKeyId"].(string)

	rec := doTransferRequest(t, h, "DescribeHostKey", map[string]any{
		"ServerId":  serverID,
		"HostKeyId": hostKeyID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	hk := resp["HostKey"].(map[string]any)

	fp, hasFp := hk["HostKeyFingerprint"].(string)
	assert.True(t, hasFp, "HostKeyFingerprint must be present in DescribeHostKey response")
	assert.Contains(t, fp, "SHA256:", "HostKeyFingerprint must start with SHA256:")

	dateImported, hasDate := hk["DateImported"].(string)
	assert.True(t, hasDate, "DateImported must be present in DescribeHostKey response")
	assert.NotEmpty(t, dateImported)

	arn, hasArn := hk["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in DescribeHostKey response")
	assert.Contains(t, arn, hostKeyID, "Arn must contain HostKeyId")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")

	tags, hasTags := hk["Tags"].([]any)
	assert.True(t, hasTags, "Tags must be present in DescribeHostKey response")
	assert.Len(t, tags, 1)
}

func TestAudit2_ListHostKeys_IncludesFingerprintAndArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	serverRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, serverRec.Code)
	var serverResp map[string]any
	require.NoError(t, json.Unmarshal(serverRec.Body.Bytes(), &serverResp))
	serverID := serverResp["ServerId"].(string)

	doTransferRequest(t, h, "ImportHostKey", map[string]any{
		"ServerId":    serverID,
		"HostKeyBody": testHostKeyEd25519,
		"Description": "list test key",
	})

	rec := doTransferRequest(t, h, "ListHostKeys", map[string]any{
		"ServerId": serverID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	hostKeys := resp["HostKeys"].([]any)
	require.NotEmpty(t, hostKeys)

	item := hostKeys[0].(map[string]any)

	fp, hasFp := item["HostKeyFingerprint"].(string)
	assert.True(t, hasFp, "HostKeyFingerprint must be present in ListHostKeys items")
	assert.Contains(t, fp, "SHA256:", "HostKeyFingerprint must start with SHA256:")

	dateImported, hasDate := item["DateImported"].(string)
	assert.True(t, hasDate, "DateImported must be present in ListHostKeys items")
	assert.NotEmpty(t, dateImported)

	arn, hasArn := item["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in ListHostKeys items")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")
}

func TestAudit2_HostKeyFingerprint_ComputedOnImport(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	hk, err := b.ImportHostKey(s.ServerID, testHostKeyEd25519, "fp test", nil)
	require.NoError(t, err)
	assert.Contains(t, hk.Fingerprint, "SHA256:", "Fingerprint must be SHA256:")
}

// ---------------------------------------------------------------------------
// Profile ARN format validation
// ---------------------------------------------------------------------------

func TestAudit2_ProfileARN_ContainsAccountAndRegion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id":       "AS2-x",
		"ProfileType": "PARTNER",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	profileID := createResp["ProfileId"].(string)

	rec := doTransferRequest(t, h, "DescribeProfile", map[string]any{
		"ProfileId": profileID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	profile := resp["Profile"].(map[string]any)

	// newTestHandler uses testAccountID = "123456789012" and testRegion = "us-east-1"
	arn := profile["Arn"].(string)
	assert.Contains(t, arn, "123456789012")
	assert.Contains(t, arn, "us-east-1")
	assert.Contains(t, arn, "profile/"+profileID)
}

// ---------------------------------------------------------------------------
// Certificate ARN format validation
// ---------------------------------------------------------------------------

func TestAudit2_CertificateARN_ContainsAccountAndRegion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage": "SIGNING",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	certID := createResp["CertificateId"].(string)

	rec := doTransferRequest(t, h, "DescribeCertificate", map[string]any{
		"CertificateId": certID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	cert := resp["Certificate"].(map[string]any)

	// newTestHandler uses testAccountID = "123456789012" and testRegion = "us-east-1"
	arn := cert["Arn"].(string)
	assert.Contains(t, arn, "123456789012")
	assert.Contains(t, arn, "us-east-1")
	assert.Contains(t, arn, "certificate/"+certID)
}
