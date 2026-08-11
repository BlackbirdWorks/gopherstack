package transfer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestHandler_UpdateConnectorLoggingRoleAndSecurityPolicyName verifies that UpdateConnector
// persists LoggingRole and SecurityPolicyName. Real AWS returns both in DescribeConnector.
func TestHandler_UpdateConnectorLoggingRoleAndSecurityPolicyName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateConnector", map[string]any{
		"Url": "https://partner.example.com",
		"As2Config": map[string]any{
			"LocalProfileId":      "local-profile",
			"PartnerProfileId":    "partner-profile",
			"MessageSubject":      "test",
			"Compression":         "DISABLED",
			"EncryptionAlgorithm": "AES128_CBC",
			"SigningAlgorithm":    "SHA256",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code, "CreateConnector failed: %s", createRec.Body.String())

	var createOut struct {
		ConnectorID string `json:"ConnectorId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	updateRec := doTransferRequest(t, h, "UpdateConnector", map[string]any{
		"ConnectorId":        createOut.ConnectorID,
		"LoggingRole":        "arn:aws:iam::123456789012:role/TransferLogging",
		"SecurityPolicyName": "TransferSecurityPolicy-2024-01",
	})
	require.Equal(t, http.StatusOK, updateRec.Code, "UpdateConnector failed: %s", updateRec.Body.String())

	descRec := doTransferRequest(t, h, "DescribeConnector", map[string]any{
		"ConnectorId": createOut.ConnectorID,
	})
	require.Equal(t, http.StatusOK, descRec.Code, "DescribeConnector failed: %s", descRec.Body.String())

	var descOut struct {
		Connector struct {
			LoggingRole        string `json:"LoggingRole"`
			SecurityPolicyName string `json:"SecurityPolicyName"`
		} `json:"Connector"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	assert.Equal(t, "arn:aws:iam::123456789012:role/TransferLogging", descOut.Connector.LoggingRole)
	assert.Equal(t, "TransferSecurityPolicy-2024-01", descOut.Connector.SecurityPolicyName)
}

// TestHandler_ConnectorURLValidation verifies HTTP returns 400 for missing URL.
func TestHandler_ConnectorURLValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateConnector", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateConnector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success with access role",
			body: map[string]any{
				"Url":        "https://partner.example.com",
				"AccessRole": "arn:aws:iam::123456789012:role/connector-role",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "success minimal",
			body:     map[string]any{"Url": "https://minimal.example.com"},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing url",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTransferRequest(t, h, "CreateConnector", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["ConnectorId"])
			}
		})
	}
}

func TestHandler_DeleteConnector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*transfer.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *transfer.Handler) string {
				c, _ := h.Backend.CreateConnector("https://example.com", "", nil, nil, nil)

				return c.ConnectorID
			},
			wantCode: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *transfer.Handler) string {
				return "c-doesnotexist"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing connector id",
			setup: func(_ *transfer.Handler) string {
				return ""
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connectorID := tt.setup(h)

			rec := doTransferRequest(t, h, "DeleteConnector", map[string]any{
				"ConnectorId": connectorID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

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
		"RemoteDirectoryPath": "/",
		"OutputDirectoryPath": "/output",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["ListingId"], "ListingId is a required field on StartDirectoryListingOutput")
	assert.Equal(t, connectorID+"-"+resp["ListingId"].(string)+".json", resp["OutputFileName"])
}

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

// Test 13: StartFileTransfer; ListFileTransferResults shows the record.
func TestHandler_StartFileTransferPersistsRecord(t *testing.T) {
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

// Test 21: DescribeConnector includes LoggingRole.
func TestHandler_DescribeConnectorLoggingRole(t *testing.T) {
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

// Test 28: StartDirectoryListing returns a unique ListingId.
func TestHandler_StartDirectoryListingUniqueId(t *testing.T) {
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

	body := map[string]any{
		"ConnectorId":         conn.ConnectorID,
		"RemoteDirectoryPath": "/remote",
		"OutputDirectoryPath": "/output",
	}

	rec1 := doTransferRequest(t, h, "StartDirectoryListing", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doTransferRequest(t, h, "StartDirectoryListing", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	var r1, r2 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))

	id1 := r1["ListingId"].(string)
	id2 := r2["ListingId"].(string)
	assert.NotEqual(t, id1, id2, "each StartDirectoryListing must return a distinct ID")
}

// TestHandler_StartDirectoryListing_RequiresFields verifies real-AWS required-field
// validation: ConnectorId, OutputDirectoryPath, and RemoteDirectoryPath are all
// required members of StartDirectoryListingInput.
func TestHandler_StartDirectoryListing_RequiresFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	conn, err := h.Backend.CreateConnector(
		"sftp://example.com", "arn:aws:iam::000000000000:role/transfer", nil, nil, nil,
	)
	require.NoError(t, err)

	tests := []struct {
		body map[string]any
		name string
	}{
		{name: "missing ConnectorId", body: map[string]any{
			"OutputDirectoryPath": "/out", "RemoteDirectoryPath": "/remote",
		}},
		{name: "missing OutputDirectoryPath", body: map[string]any{
			"ConnectorId": conn.ConnectorID, "RemoteDirectoryPath": "/remote",
		}},
		{name: "missing RemoteDirectoryPath", body: map[string]any{
			"ConnectorId": conn.ConnectorID, "OutputDirectoryPath": "/out",
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doTransferRequest(t, h, "StartDirectoryListing", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestHandler_StartRemoteDelete_ReturnsDeleteId verifies the real-AWS response key
// (DeleteId, not the invented TransferId) and that DeletePath is required.
func TestHandler_StartRemoteDelete_ReturnsDeleteId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	conn, err := h.Backend.CreateConnector(
		"sftp://example.com", "arn:aws:iam::000000000000:role/transfer", nil, nil, nil,
	)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "StartRemoteDelete", map[string]any{
		"ConnectorId": conn.ConnectorID,
		"DeletePath":  "/remote/path",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["DeleteId"])
	assert.NotContains(t, resp, "TransferId", "StartRemoteDeleteOutput has no TransferId field in real AWS")

	missingPath := doTransferRequest(t, h, "StartRemoteDelete", map[string]any{"ConnectorId": conn.ConnectorID})
	assert.Equal(t, http.StatusBadRequest, missingPath.Code)
}

// TestHandler_StartRemoteMove_ReturnsMoveId verifies the real-AWS response key
// (MoveId, not the invented TransferId) and that SourcePath/TargetPath are required.
func TestHandler_StartRemoteMove_ReturnsMoveId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	conn, err := h.Backend.CreateConnector(
		"sftp://example.com", "arn:aws:iam::000000000000:role/transfer", nil, nil, nil,
	)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "StartRemoteMove", map[string]any{
		"ConnectorId": conn.ConnectorID,
		"SourcePath":  "/source",
		"TargetPath":  "/target",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["MoveId"])
	assert.NotContains(t, resp, "TransferId", "StartRemoteMoveOutput has no TransferId field in real AWS")

	missingTarget := doTransferRequest(t, h, "StartRemoteMove", map[string]any{
		"ConnectorId": conn.ConnectorID, "SourcePath": "/source",
	})
	assert.Equal(t, http.StatusBadRequest, missingTarget.Code)
}

// TestHandler_ConnectorIPAddressType verifies IpAddressType (Connector gained this
// field in aws-sdk-go-v2/service/transfer@v1.75.4, types/types.go:720) round-trips
// through CreateConnector/DescribeConnector when supplied, and is absent from the
// raw DescribeConnector body -- not merely zero-valued -- when never set.
func TestHandler_ConnectorIPAddressType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		ipAddressType string
	}{
		{name: "ipv4", ipAddressType: "IPV4"},
		{name: "dualstack", ipAddressType: "DUALSTACK"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createRec := doTransferRequest(t, h, "CreateConnector", map[string]any{
				"Url":           "https://partner.example.com",
				"IpAddressType": tt.ipAddressType,
			})
			require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

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
			assert.Equal(t, tt.ipAddressType, connector["IpAddressType"])
			assert.Contains(t, descRec.Body.String(), `"IpAddressType":"`+tt.ipAddressType+`"`)
		})
	}

	t.Run("unset is absent from wire, not empty", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createRec := doTransferRequest(t, h, "CreateConnector", map[string]any{
			"Url": "https://partner.example.com",
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
		_, hasIPAddressType := connector["IpAddressType"]
		assert.False(t, hasIPAddressType, "IpAddressType must be absent, not an empty string, when unset")
		assert.NotContains(t, descRec.Body.String(), "IpAddressType")
	})
}

// TestHandler_UpdateConnectorIPAddressType verifies UpdateConnector persists
// IpAddressType (UpdateConnectorInput gained this field alongside CreateConnectorInput
// in aws-sdk-go-v2/service/transfer@v1.75.4, api_op_UpdateConnector.go:80).
func TestHandler_UpdateConnectorIPAddressType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doTransferRequest(t, h, "CreateConnector", map[string]any{
		"Url":           "https://partner.example.com",
		"IpAddressType": "IPV4",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	connectorID := createResp["ConnectorId"].(string)

	updateRec := doTransferRequest(t, h, "UpdateConnector", map[string]any{
		"ConnectorId":   connectorID,
		"IpAddressType": "DUALSTACK",
	})
	require.Equal(t, http.StatusOK, updateRec.Code, updateRec.Body.String())

	descRec := doTransferRequest(t, h, "DescribeConnector", map[string]any{
		"ConnectorId": connectorID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	connector := descResp["Connector"].(map[string]any)
	assert.Equal(t, "DUALSTACK", connector["IpAddressType"])
}

// TestHandler_ListConnectorsExcludesIPAddressType pins that ListedConnector (unlike
// DescribedConnector) has no IpAddressType field in real AWS -- confirmed absent from
// both types.ListedConnector and its deserializer in
// aws-sdk-go-v2/service/transfer@v1.75.4 (types/types.go:1897, deserializers.go).
func TestHandler_ListConnectorsExcludesIPAddressType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doTransferRequest(t, h, "CreateConnector", map[string]any{
		"Url":           "https://partner.example.com",
		"IpAddressType": "DUALSTACK",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	listRec := doTransferRequest(t, h, "ListConnectors", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)
	assert.NotContains(t, listRec.Body.String(), "IpAddressType",
		"ListedConnector has no IpAddressType field in real AWS")
}
