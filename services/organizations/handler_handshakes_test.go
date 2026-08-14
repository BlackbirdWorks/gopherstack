package organizations_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/organizations"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandshakeOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create org first
	doRequest(t, h, "CreateOrganization", map[string]any{"featureSet": "ALL"})

	// InviteAccountToOrganization
	rec := doRequest(t, h, "InviteAccountToOrganization", map[string]any{
		"target": map[string]any{
			"id":   "123456789013",
			"type": "ACCOUNT",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var inviteResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&inviteResp))
	invited := inviteResp["Handshake"].(map[string]any)
	handshakeID, ok := invited["Id"].(string)
	require.True(t, ok, "InviteAccountToOrganization must return a Handshake.Id")

	// ListHandshakesForOrganization must include the invite just created.
	rec = doRequest(t, h, "ListHandshakesForOrganization", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.True(t, handshakeListContains(t, rec, handshakeID),
		"ListHandshakesForOrganization must include the just-created handshake")

	// ListHandshakesForAccount must include the same invite.
	rec = doRequest(t, h, "ListHandshakesForAccount", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.True(t, handshakeListContains(t, rec, handshakeID),
		"ListHandshakesForAccount must include the just-created handshake")
}

func handshakeListContains(t *testing.T, rec *httptest.ResponseRecorder, handshakeID string) bool {
	t.Helper()

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	handshakes, ok := resp["Handshakes"].([]any)
	require.True(t, ok)

	for _, h := range handshakes {
		entry, entryOK := h.(map[string]any)
		if entryOK && entry["Id"] == handshakeID {
			return true
		}
	}

	return false
}

// TestEnableAllFeatures tests that EnableAllFeatures returns a Handshake.
func TestEnableAllFeatures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantAction string
		wantState  string
		wantStatus int
		createOrg  bool
	}{
		{
			name:       "returns_handshake",
			createOrg:  true,
			wantStatus: http.StatusOK,
			wantAction: "ENABLE_ALL_FEATURES",
			wantState:  "OPEN",
		},
		{
			name:       "no_org_error",
			createOrg:  false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.createOrg {
				doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
			}

			rec := doRequest(t, h, "EnableAllFeatures", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantAction != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				hs, ok := resp["Handshake"].(map[string]any)
				require.True(t, ok, "response must contain Handshake")
				assert.Equal(t, tt.wantAction, hs["Action"])
				assert.Equal(t, tt.wantState, hs["State"])
				assert.NotEmpty(t, hs["Id"])
			}
		})
	}
}

// TestHandshakeFilter_Handler tests the HTTP handler for handshake filter.
func TestHandshakeFilter_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      map[string]any
		name      string
		op        string
		wantCount int
	}{
		{
			name:      "list_for_account_no_filter",
			op:        "ListHandshakesForAccount",
			body:      map[string]any{},
			wantCount: 2,
		},
		{
			name: "list_for_account_invite_filter",
			op:   "ListHandshakesForAccount",
			body: map[string]any{
				"Filter": map[string]any{"ActionType": "INVITE"},
			},
			wantCount: 1,
		},
		{
			name:      "list_for_org_no_filter",
			op:        "ListHandshakesForOrganization",
			body:      map[string]any{},
			wantCount: 2,
		},
		{
			name: "list_for_org_enable_all_filter",
			op:   "ListHandshakesForOrganization",
			body: map[string]any{
				"Filter": map[string]any{"ActionType": "ENABLE_ALL_FEATURES"},
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			_, _, err := b.CreateOrganization("ALL")
			require.NoError(t, err)

			b.AddHandshakeInternal(&organizations.Handshake{Action: "INVITE", State: "OPEN"})
			b.AddHandshakeInternal(&organizations.Handshake{Action: "ENABLE_ALL_FEATURES", State: "OPEN"})

			h := organizations.NewHandler(b)

			rec := doRequest(t, h, tt.op, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

			handshakes := resp["Handshakes"].([]any)
			assert.Len(t, handshakes, tt.wantCount)
		})
	}
}

// TestHandler_HandshakeErrors tests handshake handler error paths.
func TestHandler_HandshakeErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "accept_not_found",
			op:         "AcceptHandshake",
			body:       map[string]any{"HandshakeId": "h-notexist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "cancel_not_found",
			op:         "CancelHandshake",
			body:       map[string]any{"HandshakeId": "h-notexist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "decline_not_found",
			op:         "DeclineHandshake",
			body:       map[string]any{"HandshakeId": "h-notexist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "describe_not_found",
			op:         "DescribeHandshake",
			body:       map[string]any{"HandshakeId": "h-notexist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invite_missing_target",
			op:         "InviteAccountToOrganization",
			body:       map[string]any{"Target": map[string]any{"Id": "", "Type": "ACCOUNT"}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestInviteAccount_ViaHandler tests party validation via handler.
func TestInviteAccount_ViaHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target     map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "valid_account_id",
			target:     map[string]any{"Id": "123456789012", "Type": "ACCOUNT"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_account_id_short",
			target:     map[string]any{"Id": "12345", "Type": "ACCOUNT"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "valid_email",
			target:     map[string]any{"Id": "user@example.com", "Type": "EMAIL"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_email_no_at",
			target:     map[string]any{"Id": "notanemail", "Type": "EMAIL"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doRequest(t, h, "InviteAccountToOrganization", map[string]any{
				"Target": tt.target,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandshakeExpiration_ViaHandler tests handshake expiration via handler.
func TestHandshakeExpiration_ViaHandler(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)
	h := organizations.NewHandler(b)

	// Seed an expired handshake.
	expiredHandshake := &organizations.Handshake{
		ID:                  "h-expired01",
		Action:              "INVITE",
		State:               "OPEN",
		ExpirationTimestamp: time.Now().Add(-2 * time.Hour),
		RequestedTimestamp:  time.Now().Add(-3 * time.Hour),
	}
	b.AddHandshakeInternal(expiredHandshake)

	// DescribeHandshake should show EXPIRED.
	rec := doRequest(t, h, "DescribeHandshake", map[string]any{
		"HandshakeId": "h-expired01",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	handshake := resp["Handshake"].(map[string]any)
	assert.Equal(t, "EXPIRED", handshake["State"].(string))
}

// TestInviteAccount_DuplicateOpen_ViaHandler verifies HTTP 400 +
// DuplicateHandshakeException for a duplicate open invitation.
func TestInviteAccount_DuplicateOpen_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

	target := map[string]any{"Id": "222222222222", "Type": "ACCOUNT"}

	r1 := doRequest(t, h, "InviteAccountToOrganization", map[string]any{"Target": target})
	require.Equal(t, http.StatusOK, r1.Code)

	r2 := doRequest(t, h, "InviteAccountToOrganization", map[string]any{"Target": target})
	assert.Equal(t, http.StatusBadRequest, r2.Code)

	var errResp map[string]string
	require.NoError(t, json.Unmarshal(r2.Body.Bytes(), &errResp))
	assert.Equal(t, "DuplicateHandshakeException", errResp["__type"])
}

// TestHandler_EnableAllFeatures tests the HTTP handler for EnableAllFeatures.
func TestHandler_EnableAllFeatures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "enables_all_features",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			rec := doRequest(t, h, "EnableAllFeatures", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_HandshakeLifecycle tests accept/cancel/decline/describe handshake operations.
func TestHandler_HandshakeLifecycle(t *testing.T) {
	t.Parallel()

	makeHandshake := func(id, state string) *organizations.Handshake {
		now := time.Now()

		return &organizations.Handshake{
			ID:                  id,
			ARN:                 "arn:aws:organizations::123456789012:handshake/o-test/invite/" + id,
			Action:              "INVITE",
			State:               state,
			RequestedTimestamp:  now,
			ExpirationTimestamp: now.Add(7 * 24 * time.Hour),
			Parties: []organizations.HandshakeParty{
				{ID: "123456789012", Type: "ORGANIZATION"},
				{ID: "target@example.com", Type: "EMAIL"},
			},
			Resources: []organizations.HandshakeResource{
				{Type: "ORGANIZATION", Value: "o-test"},
			},
		}
	}

	tests := []struct {
		name        string
		op          string
		handshakeID string
		seedState   string
		wantState   string
		wantStatus  int
		wantErr     bool
	}{
		{
			name:        "describe_open_handshake",
			op:          "DescribeHandshake",
			handshakeID: "h-desc01",
			seedState:   "OPEN",
			wantStatus:  http.StatusOK,
			wantState:   "OPEN",
		},
		{
			name:        "accept_open_handshake",
			op:          "AcceptHandshake",
			handshakeID: "h-accept1",
			seedState:   "OPEN",
			wantStatus:  http.StatusOK,
			wantState:   "ACCEPTED",
		},
		{
			name:        "cancel_open_handshake",
			op:          "CancelHandshake",
			handshakeID: "h-cancel1",
			seedState:   "OPEN",
			wantStatus:  http.StatusOK,
			wantState:   "CANCELED",
		},
		{
			name:        "decline_open_handshake",
			op:          "DeclineHandshake",
			handshakeID: "h-decl01",
			seedState:   "OPEN",
			wantStatus:  http.StatusOK,
			wantState:   "DECLINED",
		},
		{
			name:        "accept_already_accepted_handshake",
			op:          "AcceptHandshake",
			handshakeID: "h-aa0001",
			seedState:   "ACCEPTED",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "cancel_already_canceled_handshake",
			op:          "CancelHandshake",
			handshakeID: "h-cc0001",
			seedState:   "CANCELED",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "decline_already_declined_handshake",
			op:          "DeclineHandshake",
			handshakeID: "h-dd0001",
			seedState:   "DECLINED",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "describe_nonexistent_handshake",
			op:          "DescribeHandshake",
			handshakeID: "h-missing",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "accept_nonexistent_handshake",
			op:          "AcceptHandshake",
			handshakeID: "h-missing2",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "missing_handshake_id",
			op:          "AcceptHandshake",
			handshakeID: "",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			h := organizations.NewHandler(b)

			if tt.seedState != "" {
				hs := makeHandshake(tt.handshakeID, tt.seedState)
				b.AddHandshakeInternal(hs)
			}

			body := map[string]any{}
			if tt.handshakeID != "" {
				body["HandshakeId"] = tt.handshakeID
			}

			rec := doRequest(t, h, tt.op, body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantState != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

				hs, ok := resp["Handshake"].(map[string]any)
				require.True(t, ok, "response must have Handshake field")
				assert.Equal(t, tt.wantState, hs["State"])
				assert.Equal(t, tt.handshakeID, hs["Id"])
			}
		})
	}
}

// TestHandler_DescribeResponsibilityTransfer tests the DescribeResponsibilityTransfer operation.
func TestHandler_DescribeResponsibilityTransfer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		transferID string
		seed       bool
		wantStatus int
	}{
		{
			name:       "found",
			transferID: "rt-00000001",
			seed:       true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			transferID: "rt-missing",
			seed:       false,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_id",
			transferID: "",
			seed:       false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			h := organizations.NewHandler(b)

			if tt.seed {
				b.AddResponsibilityTransferInternal(&organizations.ResponsibilityTransfer{
					ID:                tt.transferID,
					ARN:               "arn:aws:organizations::123456789012:transfer/o-test/billing/outbound/" + tt.transferID,
					ActiveHandshakeID: "h-rt00001",
					Name:              "billing-transfer",
					Status:            "REQUESTED",
					Type:              "BILLING",
				})
			}

			body := map[string]any{}
			if tt.transferID != "" {
				body["Id"] = tt.transferID
			}

			rec := doRequest(t, h, "DescribeResponsibilityTransfer", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				details, ok := resp["ResponsibilityTransfer"].(map[string]any)
				require.True(t, ok, "response must have ResponsibilityTransfer")
				assert.Equal(t, tt.transferID, details["Id"])
				assert.Equal(t, "billing-transfer", details["Name"])
				assert.Equal(t, "REQUESTED", details["Status"])
			}
		})
	}
}

// TestHandler_BadJSON tests that bad JSON bodies return 400 for new operations.
func TestHandler_BadJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
	}{
		{name: "accept_handshake_bad_json", op: "AcceptHandshake"},
		{name: "cancel_handshake_bad_json", op: "CancelHandshake"},
		{name: "decline_handshake_bad_json", op: "DeclineHandshake"},
		{name: "describe_handshake_bad_json", op: "DescribeHandshake"},
		{name: "describe_responsibility_transfer_bad_json", op: "DescribeResponsibilityTransfer"},
		{name: "close_account_bad_json", op: "CloseAccount"},
		{name: "create_govcloud_account_bad_json", op: "CreateGovCloudAccount"},
		{name: "describe_effective_policy_bad_json", op: "DescribeEffectivePolicy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte("not-json{{{{")))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "AWSOrganizationsV20161128."+tt.op)

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}
