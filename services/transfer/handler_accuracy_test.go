package transfer_test

// handler_accuracy_test.go exercises AWS-accuracy gaps addressed in #1855.
// Each test corresponds to a specific gap or behaviour that must match real AWS.

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validTestCertPEM is a self-signed certificate used to test PEM parsing.
const validTestCertPEM = `-----BEGIN CERTIFICATE-----
MIIC/zCCAeegAwIBAgIUfG/4OODArqXXrUB2bYyGYAIeWIMwDQYJKoZIhvcNAQEL
BQAwDzENMAsGA1UEAwwEdGVzdDAeFw0yNjA1MTcxOTM2NThaFw0yNzA1MTcxOTM2
NThaMA8xDTALBgNVBAMMBHRlc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
AoIBAQC1oEoKtOIzJESaXBa6XuCUVU4vfafDlAvGUwxTyARcd+xB7QGT7Y3ilvFM
kv9Ha5bIcUtppZYTI/t/+5ZFF1By6WIpJcWeKUm5ueXDf5LKZOC2M8ANMBFgRmll
bi5hyvyf0WcK7G07n0eNIV6vfPSvzWbJuOplHPlyj6wgyfglbCUoXRdvYxv9vrKc
19wI/VKqjRrgHeQ8Kjgs/Do9U2oQIes2810Dm/haGF4bb65AdoowFPwKAfzRbxq3
U+T0RKdkMg+N3+hCUCJyTOhjiGNzl0Pe1vHqYJJk+gCy0KPZtJqXx5VWtQOsv63c
GJgyXGQtF9upqlUfazkGMwV5bmFbAgMBAAGjUzBRMB0GA1UdDgQWBBRRJauAqr0R
xOkIKwIZzJ4ag+95oTAfBgNVHSMEGDAWgBRRJauAqr0RxOkIKwIZzJ4ag+95oTAP
BgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBbHxRRXX7imnCLlVzh
BWb6rQRxMMDb214ppKkMrYacAWJUs8O35DYf/CHoh+lqp9pUX41AJS5oHnSJIVLI
n4X3/Fk3mi4bGYfqeegRC+QnxNMnHVacDg6zht/4PZxb6f+QmXq+nSgXCjrUO8rY
5XQsPS4p8itAYDPnpfrj9FyUlG8mVvkylVO3A6F+LwmJZwFBqMlX798MOHtyKNbn
Iuht/fgge8OmzBTrL5qWmS3JGFFRaWZyIuU2FrOZ/LQJqY7Pgj+WxmR3VsSr82an
BDvp/A/LIWFKk/yswpG/O5gw0vDilojojeyB9A/tcOW36xNgygOKan7YSu8P3t7s
s7B6
-----END CERTIFICATE-----`

// --- Gap 1: IdentityProviderType ---

// Test 1: CreateServer with IdentityProviderType=API_GATEWAY is echoed by DescribeServer.
func TestAccuracy_CreateServer_IdentityProviderType_APIGateway(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateServer", map[string]any{
		"IdentityProviderType": "API_GATEWAY",
		"IdentityProviderDetails": map[string]any{
			"Url":            "https://example.com/invoke",
			"InvocationRole": "arn:aws:iam::000000000000:role/test",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	descRec := doTransferRequest(t, h, "DescribeServer", map[string]any{
		"ServerId": serverID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	server := descResp["Server"].(map[string]any)

	assert.Equal(t, "API_GATEWAY", server["IdentityProviderType"])
	ipd := server["IdentityProviderDetails"].(map[string]any)
	assert.Equal(t, "https://example.com/invoke", ipd["Url"])
}

// Test 2: CreateServer with invalid IdentityProviderType returns 400.
func TestAccuracy_CreateServer_InvalidIdentityProviderType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateServer", map[string]any{
		"IdentityProviderType": "BOGUS_TYPE",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// Test 3: CreateServer with Domain=EFS is echoed by DescribeServer.
func TestAccuracy_CreateServer_DomainEFS(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateServer", map[string]any{
		"Domain": "EFS",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	descRec := doTransferRequest(t, h, "DescribeServer", map[string]any{
		"ServerId": serverID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	server := descResp["Server"].(map[string]any)

	assert.Equal(t, "EFS", server["Domain"])
}

// --- Gap 2: LoggingRole + banners ---

// Test 4: UpdateServer sets LoggingRole and pre/post auth banners; DescribeServer echoes them.
func TestAccuracy_UpdateServer_LoggingRoleAndBanners(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	updateRec := doTransferRequest(t, h, "UpdateServer", map[string]any{
		"ServerId":                      serverID,
		"LoggingRole":                   "arn:aws:iam::000000000000:role/logging",
		"PreAuthenticationLoginBanner":  "Welcome!",
		"PostAuthenticationLoginBanner": "Logged in successfully.",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	descRec := doTransferRequest(t, h, "DescribeServer", map[string]any{
		"ServerId": serverID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	server := descResp["Server"].(map[string]any)

	assert.Equal(t, "arn:aws:iam::000000000000:role/logging", server["LoggingRole"])
	assert.Equal(t, "Welcome!", server["PreAuthenticationLoginBanner"])
	assert.Equal(t, "Logged in successfully.", server["PostAuthenticationLoginBanner"])
}

// --- Gap 9: Server state lifecycle STARTING/STOPPING ---

// Test 5: StartServer transitions through STARTING before reaching ONLINE.
func TestAccuracy_StartServer_StartingState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	// Stop the server first so we can start it.
	stopRec := doTransferRequest(t, h, "StopServer", map[string]any{"ServerId": serverID})
	require.Equal(t, http.StatusOK, stopRec.Code)

	// Poll until OFFLINE.
	var state string
	for range 30 {
		descRec := doTransferRequest(t, h, "DescribeServer", map[string]any{"ServerId": serverID})
		var resp map[string]any
		_ = json.Unmarshal(descRec.Body.Bytes(), &resp)
		state = resp["Server"].(map[string]any)["State"].(string)
		if state == "OFFLINE" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	require.Equal(t, "OFFLINE", state)

	// Start the server: immediate state should be STARTING.
	startRec := doTransferRequest(t, h, "StartServer", map[string]any{"ServerId": serverID})
	require.Equal(t, http.StatusOK, startRec.Code)

	// Check immediately — should be STARTING (async transition hasn't fired yet).
	descRec := doTransferRequest(t, h, "DescribeServer", map[string]any{"ServerId": serverID})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	immediateState := descResp["Server"].(map[string]any)["State"].(string)
	assert.Equal(t, "STARTING", immediateState)

	// Poll until ONLINE.
	for range 30 {
		descRec = doTransferRequest(t, h, "DescribeServer", map[string]any{"ServerId": serverID})
		_ = json.Unmarshal(descRec.Body.Bytes(), &descResp)
		state = descResp["Server"].(map[string]any)["State"].(string)
		if state == "ONLINE" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	assert.Equal(t, "ONLINE", state)
}

// --- Gap 11: User PosixProfile ---

// Test 6: CreateUser with PosixProfile; DescribeUser echoes it.
func TestAccuracy_CreateUser_PosixProfile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "CreateUser", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "posixuser",
		"Role":     "arn:aws:iam::000000000000:role/transfer",
		"PosixProfile": map[string]any{
			"Uid":           1000,
			"Gid":           1001,
			"SecondaryGids": []int64{2000, 2001},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doTransferRequest(t, h, "DescribeUser", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "posixuser",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	user := descResp["User"].(map[string]any)
	posix := user["PosixProfile"].(map[string]any)

	assert.EqualValues(t, 1000, posix["Uid"])
	assert.EqualValues(t, 1001, posix["Gid"])
}

// Test 7: CreateUser with HomeDirectoryType=LOGICAL + mappings; DescribeUser echoes them.
func TestAccuracy_CreateUser_HomeDirectoryLogical(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "CreateUser", map[string]any{
		"ServerId":          s.ServerID,
		"UserName":          "logicaluser",
		"Role":              "arn:aws:iam::000000000000:role/transfer",
		"HomeDirectoryType": "LOGICAL",
		"HomeDirectoryMappings": []map[string]any{
			{"Entry": "/docs", "Target": "/bucket/docs"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doTransferRequest(t, h, "DescribeUser", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "logicaluser",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	user := descResp["User"].(map[string]any)

	assert.Equal(t, "LOGICAL", user["HomeDirectoryType"])
	mappings := user["HomeDirectoryMappings"].([]any)
	require.Len(t, mappings, 1)
	m := mappings[0].(map[string]any)
	assert.Equal(t, "/docs", m["Entry"])
	assert.Equal(t, "/bucket/docs", m["Target"])
}

// --- Gap 12: DescribeUser includes SshPublicKeys ---

// Test 8: DescribeUser includes SshPublicKeys list after ImportSshPublicKey.
func TestAccuracy_DescribeUser_IncludesSshPublicKeys(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = h.Backend.CreateUser(
		s.ServerID,
		"keyuser",
		"/home/keyuser",
		"arn:aws:iam::000000000000:role/transfer",
		nil,
	)
	require.NoError(t, err)

	// Import a real-looking SSH public key (ed25519 test key).
	importRec := doTransferRequest(t, h, "ImportSshPublicKey", map[string]any{
		"ServerId":         s.ServerID,
		"UserName":         "keyuser",
		"SshPublicKeyBody": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl test@example",
	})
	require.Equal(t, http.StatusOK, importRec.Code)

	descRec := doTransferRequest(t, h, "DescribeUser", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "keyuser",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	user := descResp["User"].(map[string]any)
	keys, ok := user["SshPublicKeys"].([]any)
	require.True(t, ok, "SshPublicKeys must be present")
	assert.Len(t, keys, 1)
}

// --- Gap 13: ImportSshPublicKey duplicate detection ---

// Test 9: ImportSshPublicKey with duplicate body returns ResourceExistsException.
func TestAccuracy_ImportSshPublicKey_DuplicateBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)
	_, err = h.Backend.CreateUser(s.ServerID, "dupuser", "/", "arn:aws:iam::000000000000:role/transfer", nil)
	require.NoError(t, err)

	keyBody := "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl dup@test"

	firstRec := doTransferRequest(t, h, "ImportSshPublicKey", map[string]any{
		"ServerId":         s.ServerID,
		"UserName":         "dupuser",
		"SshPublicKeyBody": keyBody,
	})
	require.Equal(t, http.StatusOK, firstRec.Code)

	// Second import of the same body must fail.
	secondRec := doTransferRequest(t, h, "ImportSshPublicKey", map[string]any{
		"ServerId":         s.ServerID,
		"UserName":         "dupuser",
		"SshPublicKeyBody": keyBody,
	})
	assert.Equal(t, http.StatusBadRequest, secondRec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(secondRec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceExistsException", errResp["__type"])
}

// --- Gap 14: Access full shape ---

// Test 10: CreateAccess with PosixProfile; DescribeAccess echoes it.
func TestAccuracy_CreateAccess_PosixProfile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "CreateAccess", map[string]any{
		"ServerId":   s.ServerID,
		"ExternalId": "S-1-5-21-access",
		"Role":       "arn:aws:iam::000000000000:role/transfer",
		"PosixProfile": map[string]any{
			"Uid": 500,
			"Gid": 501,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doTransferRequest(t, h, "DescribeAccess", map[string]any{
		"ServerId":   s.ServerID,
		"ExternalId": "S-1-5-21-access",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	access := descResp["Access"].(map[string]any)
	posix := access["PosixProfile"].(map[string]any)

	assert.EqualValues(t, 500, posix["Uid"])
	assert.EqualValues(t, 501, posix["Gid"])
}

// --- Gap 15: Agreement status enum + enriched list ---

// Test 11: ListAgreements response includes Arn + LocalProfileId + PartnerProfileId.
func TestAccuracy_ListAgreements_EnrichedFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = h.Backend.CreateAgreementFull(
		s.ServerID,
		"test agreement",
		"local-profile-1",
		"partner-profile-1",
		"/base",
		"arn:aws:iam::000000000000:role/transfer",
		"ACTIVE",
		nil,
	)
	require.NoError(t, err)

	listRec := doTransferRequest(t, h, "ListAgreements", map[string]any{
		"ServerId": s.ServerID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	agreements := listResp["Agreements"].([]any)
	require.Len(t, agreements, 1)

	ag := agreements[0].(map[string]any)
	assert.NotEmpty(t, ag["Arn"])
	assert.Equal(t, "local-profile-1", ag["LocalProfileId"])
	assert.Equal(t, "partner-profile-1", ag["PartnerProfileId"])
}

// Test 12: Agreement with invalid Status on Create returns 400.
func TestAccuracy_CreateAgreement_InvalidStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "CreateAgreement", map[string]any{
		"ServerId":         s.ServerID,
		"LocalProfileId":   "lp-1",
		"PartnerProfileId": "pp-1",
		"BaseDirectory":    "/",
		"AccessRole":       "arn:aws:iam::000000000000:role/transfer",
		"Status":           "PENDING", // invalid
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Gap 17: StartFileTransfer persists record ---

// Test 13: StartFileTransfer; ListFileTransferResults shows the record.
func TestAccuracy_StartFileTransfer_PersistsRecord(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	c, err := h.Backend.CreateConnector("sftp://example.com", "arn:aws:iam::000000000000:role/transfer", nil, nil, nil)
	require.NoError(t, err)

	startRec := doTransferRequest(t, h, "StartFileTransfer", map[string]any{
		"ConnectorId":   c.ConnectorID,
		"SendFilePaths": []string{"/path/to/file.txt"},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	transferID := startResp["TransferId"].(string)
	assert.NotEmpty(t, transferID)

	listRec := doTransferRequest(t, h, "ListFileTransferResults", map[string]any{
		"ConnectorId": c.ConnectorID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	results := listResp["FileTransferResults"].([]any)
	require.NotEmpty(t, results)

	found := false
	for _, r := range results {
		result := r.(map[string]any)
		if result["TransferId"] == transferID {
			found = true
			assert.Equal(t, c.ConnectorID, result["ConnectorId"])

			break
		}
	}
	assert.True(t, found, "transfer record must appear in ListFileTransferResults")
}

// --- Gap 20: TestIdentityProvider SERVICE_MANAGED ---

// Test 14: TestIdentityProvider SERVICE_MANAGED known user returns 200.
func TestAccuracy_TestIdentityProvider_KnownUser(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)
	_, err = h.Backend.CreateUser(s.ServerID, "alice", "/home/alice", "arn:aws:iam::000000000000:role/transfer", nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "TestIdentityProvider", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "alice",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.EqualValues(t, 200, resp["StatusCode"])
}

// Test 15: TestIdentityProvider SERVICE_MANAGED unknown user returns 401.
func TestAccuracy_TestIdentityProvider_UnknownUser(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "TestIdentityProvider", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "nobody",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.EqualValues(t, 401, resp["StatusCode"])
}

// --- Gap 22: Workflow steps typed details ---

// Test 16: DescribeWorkflow includes typed CopyStepDetails.
func TestAccuracy_DescribeWorkflow_TypedCopyStepDetails(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWorkflow", map[string]any{
		"Description": "copy workflow",
		"Steps": []map[string]any{
			{
				"Type": "COPY",
				"CopyStepDetails": map[string]any{
					"Name":               "my-copy",
					"SourceFileLocation": "${original.file}",
					"OverwriteExisting":  "TRUE",
					"DestinationFileLocation": map[string]any{
						"S3FileLocation": map[string]any{
							"Bucket": "my-bucket",
							"Key":    "output/${original.file}",
						},
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	workflowID := createResp["WorkflowId"].(string)

	descRec := doTransferRequest(t, h, "DescribeWorkflow", map[string]any{
		"WorkflowId": workflowID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	wf := descResp["Workflow"].(map[string]any)
	steps := wf["Steps"].([]any)
	require.Len(t, steps, 1)

	step := steps[0].(map[string]any)
	assert.Equal(t, "COPY", step["Type"])

	copyDetails, ok := step["CopyStepDetails"].(map[string]any)
	require.True(t, ok, "CopyStepDetails must be present")
	assert.Equal(t, "my-copy", copyDetails["Name"])
	assert.Equal(t, "${original.file}", copyDetails["SourceFileLocation"])
	assert.Equal(t, "TRUE", copyDetails["OverwriteExisting"])
}

// --- Gap 23: ImportCertificate PEM parsing ---

// Test 17: ImportCertificate with malformed PEM returns 400.
func TestAccuracy_ImportCertificate_MalformedPEM(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage":       "SIGNING",
		"Certificate": "-----BEGIN CERTIFICATE-----\nNOTVALIDBASE64!!!\n-----END CERTIFICATE-----",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidRequestException", resp["__type"])
}

// Test 18: ImportCertificate with valid PEM returns NotBeforeDate/NotAfterDate.
func TestAccuracy_ImportCertificate_ValidPEM_ExtractsDates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage":       "SIGNING",
		"Certificate": validTestCertPEM,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	certID := createResp["CertificateId"].(string)

	descRec := doTransferRequest(t, h, "DescribeCertificate", map[string]any{
		"CertificateId": certID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	cert := descResp["Certificate"].(map[string]any)

	assert.NotEmpty(t, cert["NotBeforeDate"], "NotBeforeDate must be set from PEM")
	assert.NotEmpty(t, cert["NotAfterDate"], "NotAfterDate must be set from PEM")
}

// --- Gap 4: ProtocolDetails ---

// Test 19: UpdateServer with ProtocolDetails; DescribeServer echoes ProtocolDetails.
func TestAccuracy_UpdateServer_ProtocolDetails(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	updateRec := doTransferRequest(t, h, "UpdateServer", map[string]any{
		"ServerId": serverID,
		"ProtocolDetails": map[string]any{
			"PassiveIp":                "0.0.0.0",
			"TlsSessionResumptionMode": "ENFORCED",
			"SetStatOption":            "ENABLE_NO_OP",
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	descRec := doTransferRequest(t, h, "DescribeServer", map[string]any{
		"ServerId": serverID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	server := descResp["Server"].(map[string]any)
	pd := server["ProtocolDetails"].(map[string]any)

	assert.Equal(t, "0.0.0.0", pd["PassiveIp"])
	assert.Equal(t, "ENFORCED", pd["TlsSessionResumptionMode"])
	assert.Equal(t, "ENABLE_NO_OP", pd["SetStatOption"])
}

// --- Gap 25: Pagination ---

// Test 20: ListUsers with MaxResults=2 returns NextToken when more exist.
func TestAccuracy_ListUsers_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	// Create 3 users.
	for _, name := range []string{"alice", "bob", "carol"} {
		_, err = h.Backend.CreateUser(s.ServerID, name, "/home/"+name, "arn:aws:iam::000000000000:role/transfer", nil)
		require.NoError(t, err)
	}

	listRec := doTransferRequest(t, h, "ListUsers", map[string]any{
		"ServerId":   s.ServerID,
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	users := listResp["Users"].([]any)
	assert.Len(t, users, 2)
	assert.NotEmpty(t, listResp["NextToken"], "NextToken must be set when more results exist")
}

// --- Gap 16: Connector LoggingRole + SecurityPolicyName ---

// Test 21: DescribeConnector includes LoggingRole.
func TestAccuracy_DescribeConnector_LoggingRole(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateConnector", map[string]any{
		"Url":                "sftp://example.com",
		"AccessRole":         "arn:aws:iam::000000000000:role/transfer",
		"LoggingRole":        "arn:aws:iam::000000000000:role/logging",
		"SecurityPolicyName": "TransferSecurityPolicy-2024-01",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	connectorID := createResp["ConnectorId"].(string)

	descRec := doTransferRequest(t, h, "DescribeConnector", map[string]any{
		"ConnectorId": connectorID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	connector := descResp["Connector"].(map[string]any)

	assert.Equal(t, "arn:aws:iam::000000000000:role/logging", connector["LoggingRole"])
	assert.Equal(t, "TransferSecurityPolicy-2024-01", connector["SecurityPolicyName"])
}

// --- Additional accuracy tests ---

// Test 22: CreateServer with EndpointType=VPC + EndpointDetails echoed.
func TestAccuracy_CreateServer_EndpointTypeVPC(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateServer", map[string]any{
		"EndpointType": "VPC",
		"EndpointDetails": map[string]any{
			"SubnetIds":        []string{"subnet-abc123"},
			"SecurityGroupIds": []string{"sg-def456"},
			"VpcId":            "vpc-xyz789",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	descRec := doTransferRequest(t, h, "DescribeServer", map[string]any{"ServerId": serverID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	server := descResp["Server"].(map[string]any)

	assert.Equal(t, "VPC", server["EndpointType"])
	ed := server["EndpointDetails"].(map[string]any)
	assert.Equal(t, "vpc-xyz789", ed["VpcId"])
}

// Test 23: CreateServer with WorkflowDetails echoed.
func TestAccuracy_CreateServer_WorkflowDetails(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateServer", map[string]any{
		"WorkflowDetails": map[string]any{
			"OnUpload": []map[string]any{
				{"WorkflowId": "w-12345", "ExecutionRole": "arn:aws:iam::000000000000:role/wf"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	descRec := doTransferRequest(t, h, "DescribeServer", map[string]any{"ServerId": serverID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	server := descResp["Server"].(map[string]any)
	wd := server["WorkflowDetails"].(map[string]any)
	onUpload := wd["OnUpload"].([]any)
	require.Len(t, onUpload, 1)
	item := onUpload[0].(map[string]any)
	assert.Equal(t, "w-12345", item["WorkflowId"])
}

// Test 24: S3StorageOptions echoed in DescribeServer.
func TestAccuracy_CreateServer_S3StorageOptions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateServer", map[string]any{
		"S3StorageOptions": map[string]any{
			"DirectoryListingOptimization": "ENABLED",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	descRec := doTransferRequest(t, h, "DescribeServer", map[string]any{"ServerId": serverID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	server := descResp["Server"].(map[string]any)
	s3 := server["S3StorageOptions"].(map[string]any)
	assert.Equal(t, "ENABLED", s3["DirectoryListingOptimization"])
}

// Test 25: StructuredLogDestinations echoed in DescribeServer.
func TestAccuracy_CreateServer_StructuredLogDestinations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateServer", map[string]any{
		"StructuredLogDestinations": []string{"arn:aws:logs:us-east-1:000000000000:log-group:transfer"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	descRec := doTransferRequest(t, h, "DescribeServer", map[string]any{"ServerId": serverID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	server := descResp["Server"].(map[string]any)
	dests := server["StructuredLogDestinations"].([]any)
	require.Len(t, dests, 1)
	assert.Equal(t, "arn:aws:logs:us-east-1:000000000000:log-group:transfer", dests[0])
}

// Test 26: IpAddressType echoed in DescribeServer.
func TestAccuracy_CreateServer_IpAddressType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateServer", map[string]any{
		"IpAddressType": "DUALSTACK",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	descRec := doTransferRequest(t, h, "DescribeServer", map[string]any{"ServerId": serverID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	server := descResp["Server"].(map[string]any)
	assert.Equal(t, "DUALSTACK", server["IpAddressType"])
}

// Test 27: Agreement created with INACTIVE status is stored correctly.
func TestAccuracy_CreateAgreement_InactiveStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "CreateAgreement", map[string]any{
		"ServerId":         s.ServerID,
		"LocalProfileId":   "lp-1",
		"PartnerProfileId": "pp-1",
		"BaseDirectory":    "/",
		"AccessRole":       "arn:aws:iam::000000000000:role/transfer",
		"Status":           "INACTIVE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	agID := createResp["AgreementId"].(string)

	descRec := doTransferRequest(t, h, "DescribeAgreement", map[string]any{
		"ServerId":    s.ServerID,
		"AgreementId": agID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	ag := descResp["Agreement"].(map[string]any)
	assert.Equal(t, "INACTIVE", ag["Status"])
}

// Test 28: StartDirectoryListing returns a unique DirectoryListingId.
func TestAccuracy_StartDirectoryListing_UniqueId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	conn, err := h.Backend.CreateConnector(
		"sftp://example.com",
		"arn:aws:iam::000000000000:role/transfer",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	rec1 := doTransferRequest(t, h, "StartDirectoryListing", map[string]any{
		"ConnectorId":          conn.ConnectorID,
		"RemoteDirectoryPaths": []string{"/remote"},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doTransferRequest(t, h, "StartDirectoryListing", map[string]any{
		"ConnectorId":          conn.ConnectorID,
		"RemoteDirectoryPaths": []string{"/remote"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var r1, r2 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))

	id1 := r1["DirectoryListingId"].(string)
	id2 := r2["DirectoryListingId"].(string)
	assert.NotEqual(t, id1, id2, "each StartDirectoryListing must return a distinct ID")
}

// Test 29: SendWorkflowStepState COMPLETE transitions execution to COMPLETED.
func TestAccuracy_SendWorkflowStepState_Success(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wf, err := h.Backend.CreateWorkflow("accuracy test workflow", nil, nil, nil)
	require.NoError(t, err)

	exec, err := h.Backend.CreateExecution(wf.WorkflowID)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "SendWorkflowStepState", map[string]any{
		"WorkflowId":  wf.WorkflowID,
		"ExecutionId": exec.ExecutionID,
		"Token":       "tok-abc",
		"Status":      "COMPLETE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	updatedExec, err := h.Backend.DescribeExecution(wf.WorkflowID, exec.ExecutionID)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETED", updatedExec.Status)
}

// Test 30: UpdateUser PosixProfile can be updated via UpdateUser.
func TestAccuracy_UpdateUser_PosixProfile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)
	_, err = h.Backend.CreateUser(s.ServerID, "updateuser", "/home/u", "arn:aws:iam::000000000000:role/transfer", nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "UpdateUser", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "updateuser",
		"PosixProfile": map[string]any{
			"Uid": 9999,
			"Gid": 8888,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doTransferRequest(t, h, "DescribeUser", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "updateuser",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	user := descResp["User"].(map[string]any)
	posix := user["PosixProfile"].(map[string]any)
	assert.EqualValues(t, 9999, posix["Uid"])
}
