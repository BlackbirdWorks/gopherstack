package transfer_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestHandler_UpdateAccessFullFieldsRoundtrip verifies that UpdateAccess persists
// PosixProfile, HomeDirectoryType, and Policy fields. Real AWS supports all mutable
// Access fields in UpdateAccess.
func TestHandler_UpdateAccessFullFieldsRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createSrvRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, createSrvRec.Code)

	var srvOut struct {
		ServerID string `json:"ServerId"`
	}
	require.NoError(t, json.Unmarshal(createSrvRec.Body.Bytes(), &srvOut))

	createAccessRec := doTransferRequest(t, h, "CreateAccess", map[string]any{
		"ServerId":          srvOut.ServerID,
		"ExternalId":        "S-1-5-21-9999",
		"Role":              "arn:aws:iam::123456789012:role/TransferRole",
		"HomeDirectoryType": "PATH",
		"HomeDirectory":     "/home/alice",
	})
	require.Equal(t, http.StatusOK, createAccessRec.Code, "CreateAccess failed: %s", createAccessRec.Body.String())

	updateRec := doTransferRequest(t, h, "UpdateAccess", map[string]any{
		"ServerId":          srvOut.ServerID,
		"ExternalId":        "S-1-5-21-9999",
		"HomeDirectoryType": "LOGICAL",
		"Policy":            `{"Version":"2012-10-17","Statement":[]}`,
		"PosixProfile": map[string]any{
			"Uid": 1001,
			"Gid": 1001,
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code, "UpdateAccess failed: %s", updateRec.Body.String())

	descRec := doTransferRequest(t, h, "DescribeAccess", map[string]any{
		"ServerId":   srvOut.ServerID,
		"ExternalId": "S-1-5-21-9999",
	})
	require.Equal(t, http.StatusOK, descRec.Code, "DescribeAccess failed: %s", descRec.Body.String())

	var descOut struct {
		Access struct {
			PosixProfile *struct {
				UID int `json:"Uid"`
				GID int `json:"Gid"`
			} `json:"PosixProfile"`
			HomeDirectoryType string `json:"HomeDirectoryType"`
			Policy            string `json:"Policy"`
		} `json:"Access"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	assert.Equal(t, "LOGICAL", descOut.Access.HomeDirectoryType)
	assert.JSONEq(t, `{"Version":"2012-10-17","Statement":[]}`, descOut.Access.Policy)
	require.NotNil(t, descOut.Access.PosixProfile)
	assert.Equal(t, 1001, descOut.Access.PosixProfile.UID)
}

// TestHandler_CreateAccessDuplicateExternalID verifies 400 response.
func TestHandler_CreateAccessDuplicateExternalID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "CreateAccess", map[string]any{
		"ServerId":   s.ServerID,
		"ExternalId": "S-1-5-21-9999",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doTransferRequest(t, h, "CreateAccess", map[string]any{
		"ServerId":   s.ServerID,
		"ExternalId": "S-1-5-21-9999",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_ListAccessesIncludesHomeDirectoryType verifies that
// ListAccesses returns HomeDirectoryType per entry, matching real AWS's
// ListedAccess shape.
func TestHandler_ListAccessesIncludesHomeDirectoryType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	createRec := doTransferRequest(t, h, "CreateAccess", map[string]any{
		"ServerId":          s.ServerID,
		"ExternalId":        "S-1-5-21-1234",
		"HomeDirectoryType": "LOGICAL",
		"HomeDirectoryMappings": []map[string]any{
			{"Entry": "/", "Target": "/bucket/home"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	listRec := doTransferRequest(t, h, "ListAccesses", map[string]any{"ServerId": s.ServerID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	accesses := listResp["Accesses"].([]any)
	require.Len(t, accesses, 1)
	item := accesses[0].(map[string]any)
	assert.Equal(t, "LOGICAL", item["HomeDirectoryType"])
}

func TestHandler_CreateAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantKey  string
		wantCode int
	}{
		{
			name: "success",
			body: map[string]any{
				"ExternalId": "S-1-5-21-1234",
			},
			wantCode: http.StatusOK,
			wantKey:  "ExternalId",
		},
		{
			name:     "missing external id",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			s, err := h.Backend.CreateServer(nil, nil)
			require.NoError(t, err)

			body := make(map[string]any, len(tt.body)+1)
			maps.Copy(body, tt.body)
			body["ServerId"] = s.ServerID

			rec := doTransferRequest(t, h, "CreateAccess", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantKey != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp[tt.wantKey])
			}
		})
	}
}

func TestHandler_CreateAccess_MissingServerID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateAccess", map[string]any{
		"ExternalId": "S-1-5-21-1234",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateAccess_ServerNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateAccess", map[string]any{
		"ServerId":   "s-doesnotexist",
		"ExternalId": "S-1-5-21-1234",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeleteAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*transfer.Handler) (serverID, externalID string)
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *transfer.Handler) (string, string) {
				s, _ := h.Backend.CreateServer(nil, nil)
				_, _ = h.Backend.CreateAccess(s.ServerID, "S-1-5-21-1234", "", "", nil)

				return s.ServerID, "S-1-5-21-1234"
			},
			wantCode: http.StatusOK,
		},
		{
			name: "access not found",
			setup: func(h *transfer.Handler) (string, string) {
				s, _ := h.Backend.CreateServer(nil, nil)

				return s.ServerID, "S-missing"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "server not found",
			setup: func(_ *transfer.Handler) (string, string) {
				return "s-doesnotexist", "S-1-5-21-1234"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			serverID, externalID := tt.setup(h)

			rec := doTransferRequest(t, h, "DeleteAccess", map[string]any{
				"ServerId":   serverID,
				"ExternalId": externalID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DeleteAccess_MissingFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing server id",
			body:     map[string]any{"ExternalId": "S-1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing external id",
			body:     map[string]any{"ServerId": s.ServerID},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doTransferRequest(t, h, "DeleteAccess", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

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

// Test 10: CreateAccess with PosixProfile; DescribeAccess echoes it.
func TestHandler_CreateAccessPosixProfile(t *testing.T) {
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
