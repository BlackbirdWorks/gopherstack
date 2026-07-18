package transfer_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test 1: CreateServer with IdentityProviderType=API_GATEWAY is echoed by DescribeServer.
func TestHandler_CreateServerIdentityProviderTypeAPIGateway(t *testing.T) {
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
func TestHandler_CreateServerInvalidIdentityProviderType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateServer", map[string]any{
		"IdentityProviderType": "BOGUS_TYPE",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// Test 3: CreateServer with Domain=EFS is echoed by DescribeServer.
func TestHandler_CreateServerDomainEFS(t *testing.T) {
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

// Test 4: UpdateServer sets LoggingRole and pre/post auth banners; DescribeServer echoes them.
func TestHandler_UpdateServerLoggingRoleAndBanners(t *testing.T) {
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

// Test 5: StartServer transitions through STARTING before reaching ONLINE.
func TestHandler_StartServerStartingState(t *testing.T) {
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

// Test 14: TestIdentityProvider SERVICE_MANAGED known user returns 200.
func TestHandler_TestIdentityProviderKnownUser(t *testing.T) {
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
func TestHandler_TestIdentityProviderUnknownUser(t *testing.T) {
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

// Test 19: UpdateServer with ProtocolDetails; DescribeServer echoes ProtocolDetails.
func TestHandler_UpdateServerProtocolDetails(t *testing.T) {
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

// Test 22: CreateServer with EndpointType=VPC + EndpointDetails echoed.
func TestHandler_CreateServerEndpointTypeVPC(t *testing.T) {
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
func TestHandler_CreateServerWorkflowDetails(t *testing.T) {
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
func TestHandler_CreateServerS3StorageOptions(t *testing.T) {
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
func TestHandler_CreateServerStructuredLogDestinations(t *testing.T) {
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
func TestHandler_CreateServerIpAddressType(t *testing.T) {
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
