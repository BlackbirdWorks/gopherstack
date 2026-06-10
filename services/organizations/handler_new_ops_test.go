package organizations_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// TestHandler_CloseAccount tests the CloseAccount operation.
func TestHandler_CloseAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		wantStatus    int
		createAccount bool
	}{
		{
			name:          "closes_existing_account",
			createAccount: true,
			wantStatus:    http.StatusOK,
		},
		{
			name:       "missing_account_id",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "account_not_found",
			body:       map[string]any{"AccountId": "999999999999"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

			body := tt.body

			if tt.createAccount {
				// Create an account first then close it.
				acctRec := doRequest(t, h, "CreateAccount", map[string]any{
					"AccountName": "close-test",
					"Email":       "close@example.com",
				})
				require.Equal(t, http.StatusOK, acctRec.Code)

				var acctResp map[string]any
				require.NoError(t, json.NewDecoder(acctRec.Body).Decode(&acctResp))

				status := acctResp["CreateAccountStatus"].(map[string]any)
				body = map[string]any{"AccountId": status["AccountId"]}
			}

			rec := doRequest(t, h, "CloseAccount", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_CreateGovCloudAccount tests the CreateGovCloudAccount operation.
func TestHandler_CreateGovCloudAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantGovID  bool
	}{
		{
			name: "creates_govcloud_account",
			body: map[string]any{
				"AccountName": "govcloud-test",
				"Email":       "gov@example.com",
			},
			wantStatus: http.StatusOK,
			wantGovID:  true,
		},
		{
			name:       "missing_account_name",
			body:       map[string]any{"Email": "gov@example.com"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_email",
			body:       map[string]any{"AccountName": "gov-test"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

			rec := doRequest(t, h, "CreateGovCloudAccount", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantGovID {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

				status, ok := resp["CreateAccountStatus"].(map[string]any)
				require.True(t, ok, "response must have CreateAccountStatus")
				assert.NotEmpty(t, status["AccountId"])
				assert.NotEmpty(t, status["GovCloudAccountId"])
				assert.Equal(t, "SUCCEEDED", status["State"])
			}
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
		name        string
		handshakeID string
		seed        bool
		wantStatus  int
	}{
		{
			name:        "found",
			handshakeID: "h-rt00001",
			seed:        true,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "not_found",
			handshakeID: "h-missing",
			seed:        false,
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "missing_id",
			handshakeID: "",
			seed:        false,
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			h := organizations.NewHandler(b)

			if tt.seed {
				now := time.Now()
				b.AddHandshakeInternal(&organizations.Handshake{
					ID:                  tt.handshakeID,
					ARN:                 "arn:aws:organizations::123456789012:handshake/o-test/transfer/" + tt.handshakeID,
					Action:              "APPROVE_ALL_FEATURES",
					State:               "OPEN",
					RequestedTimestamp:  now,
					ExpirationTimestamp: now.Add(7 * 24 * time.Hour),
				})
			}

			body := map[string]any{}
			if tt.handshakeID != "" {
				body["HandshakeId"] = tt.handshakeID
			}

			rec := doRequest(t, h, "DescribeResponsibilityTransfer", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				details, ok := resp["HandshakeDetails"].(map[string]any)
				require.True(t, ok, "response must have HandshakeDetails")
				assert.Equal(t, tt.handshakeID, details["Id"])
			}
		})
	}
}

// TestHandler_ResourcePolicy tests DeleteResourcePolicy and DescribeResourcePolicy.
func TestHandler_ResourcePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		seedPolicy bool
		wantStatus int
	}{
		{
			name:       "describe_no_policy",
			op:         "DescribeResourcePolicy",
			seedPolicy: false,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete_no_policy",
			op:         "DeleteResourcePolicy",
			seedPolicy: false,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "describe_existing_policy",
			op:         "DescribeResourcePolicy",
			seedPolicy: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete_existing_policy",
			op:         "DeleteResourcePolicy",
			seedPolicy: true,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			h := organizations.NewHandler(b)

			// Create an organization first (required for resource policy operations).
			doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

			if tt.seedPolicy {
				// Seed via PutResourcePolicy backend method.
				_, err := b.PutResourcePolicy(`{"Version":"2012-10-17","Statement":[]}`)
				require.NoError(t, err)
			}

			rec := doRequest(t, h, tt.op, map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.op == "DescribeResourcePolicy" && tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				rp, ok := resp["ResourcePolicy"].(map[string]any)
				require.True(t, ok, "response must have ResourcePolicy")
				assert.NotEmpty(t, rp["Content"])
				summary, ok := rp["ResourcePolicySummary"].(map[string]any)
				require.True(t, ok, "response must have ResourcePolicySummary")
				assert.NotEmpty(t, summary["Id"])
				assert.NotEmpty(t, summary["Arn"])
			}
		})
	}
}

// TestHandler_DescribeEffectivePolicy tests the DescribeEffectivePolicy operation.
func TestHandler_DescribeEffectivePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		policyType  string
		targetID    string
		wantStatus  int
		seedPolicy  bool
		wantContent bool
	}{
		{
			name:       "no_policy_returns_not_found",
			policyType: "SERVICE_CONTROL_POLICY",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:        "effective_policy_found_on_target",
			policyType:  "SERVICE_CONTROL_POLICY",
			seedPolicy:  true,
			wantStatus:  http.StatusOK,
			wantContent: true,
		},
		{
			name:       "missing_policy_type",
			policyType: "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			h := organizations.NewHandler(b)

			doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

			var targetID string

			if tt.seedPolicy {
				// Create an account and attach a policy of the given type to it.
				acctRec := doRequest(t, h, "CreateAccount", map[string]any{
					"AccountName": "effective-test",
					"Email":       "eff@example.com",
				})
				require.Equal(t, http.StatusOK, acctRec.Code)

				var acctResp map[string]any
				require.NoError(t, json.NewDecoder(acctRec.Body).Decode(&acctResp))
				status := acctResp["CreateAccountStatus"].(map[string]any)
				targetID = status["AccountId"].(string)

				// Create a policy of the given type.
				policyRec := doRequest(t, h, "CreatePolicy", map[string]any{
					"Name":    "test-scp",
					"Content": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
					"Type":    tt.policyType,
				})
				require.Equal(t, http.StatusOK, policyRec.Code)

				var policyResp map[string]any
				require.NoError(t, json.NewDecoder(policyRec.Body).Decode(&policyResp))
				policyObj := policyResp["Policy"].(map[string]any)
				summary := policyObj["PolicySummary"].(map[string]any)
				policyID := summary["Id"].(string)

				// Attach the policy to the account.
				attachRec := doRequest(t, h, "AttachPolicy", map[string]any{
					"PolicyId": policyID,
					"TargetId": targetID,
				})
				require.Equal(t, http.StatusOK, attachRec.Code)
			}

			body := map[string]any{}
			if tt.policyType != "" {
				body["PolicyType"] = tt.policyType
			}

			if targetID != "" {
				body["TargetId"] = targetID
			}

			rec := doRequest(t, h, "DescribeEffectivePolicy", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantContent {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				ep, ok := resp["EffectivePolicy"].(map[string]any)
				require.True(t, ok, "response must have EffectivePolicy")
				assert.NotEmpty(t, ep["PolicyContent"])
				assert.NotEmpty(t, ep["PolicyId"])
				assert.Equal(t, tt.policyType, ep["PolicyType"])
				assert.NotZero(t, ep["LastUpdatedTimestamp"])
			}
		})
	}
}

// TestHandler_NewOps_BadJSON tests that bad JSON bodies return 400 for new operations.
func TestHandler_NewOps_BadJSON(t *testing.T) {
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

func TestBackend_CloseAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		wantErr   bool
	}{
		{
			name:    "closes_account",
			wantErr: false,
		},
		{
			name:      "not_found",
			accountID: "999999999999",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			accountID := tt.accountID

			if accountID == "" {
				status, err := b.CreateAccount("close-test", "close@example.com", "", "", nil)
				require.NoError(t, err)
				accountID = status.AccountID
			}

			err := b.CloseAccount(accountID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			// Account should now be PENDING_CLOSURE.
			acct, descErr := b.DescribeAccount(accountID)
			require.NoError(t, descErr)
			assert.Equal(t, "PENDING_CLOSURE", acct.Status)
		})
	}
}

// TestBackend_CreateGovCloudAccount tests the CreateGovCloudAccount backend method.
func TestBackend_CreateGovCloudAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		acctName string
		email    string
		noOrg    bool
		wantErr  bool
	}{
		{
			name:     "creates_govcloud_account",
			acctName: "govtest",
			email:    "gov@example.com",
		},
		{
			name:     "no_org_returns_error",
			acctName: "govtest",
			email:    "gov@example.com",
			noOrg:    true,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if !tt.noOrg {
				_, _, err := b.CreateOrganization("ALL")
				require.NoError(t, err)
			}

			status, err := b.CreateGovCloudAccount(tt.acctName, tt.email, "", "", nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, status)
			assert.NotEmpty(t, status.AccountID)
			assert.NotEmpty(t, status.GovCloudAccountID)
			assert.Equal(t, "SUCCEEDED", status.State)
			assert.Equal(t, tt.acctName, status.AccountName)
		})
	}
}

// TestBackend_HandshakeOperations tests handshake CRUD methods.
func TestBackend_HandshakeOperations(t *testing.T) {
	t.Parallel()

	makeOpenHandshake := func(id string) *organizations.Handshake {
		now := time.Now()

		return &organizations.Handshake{
			ID:                  id,
			ARN:                 "arn:aws:organizations::123456789012:handshake/o-test/invite/" + id,
			Action:              "INVITE",
			State:               "OPEN",
			RequestedTimestamp:  now,
			ExpirationTimestamp: now.Add(7 * 24 * time.Hour),
		}
	}

	tests := []struct {
		name      string
		op        string
		id        string
		seedID    string
		seedState string
		wantState string
		wantErr   bool
	}{
		{
			name:      "describe_found",
			op:        "DescribeHandshake",
			id:        "h-desc001",
			seedID:    "h-desc001",
			wantState: "OPEN",
		},
		{
			name:    "describe_not_found",
			op:      "DescribeHandshake",
			id:      "h-notfound",
			wantErr: true,
		},
		{
			name:      "accept_open",
			op:        "AcceptHandshake",
			id:        "h-acc0001",
			seedID:    "h-acc0001",
			wantState: "ACCEPTED",
		},
		{
			name:    "accept_not_found",
			op:      "AcceptHandshake",
			id:      "h-missing",
			wantErr: true,
		},
		{
			name:      "cancel_open",
			op:        "CancelHandshake",
			id:        "h-can0001",
			seedID:    "h-can0001",
			wantState: "CANCELED",
		},
		{
			name:      "decline_open",
			op:        "DeclineHandshake",
			id:        "h-dec0001",
			seedID:    "h-dec0001",
			wantState: "DECLINED",
		},
		{
			name:      "accept_already_accepted",
			op:        "AcceptHandshake",
			id:        "h-aa00001",
			seedID:    "h-aa00001",
			seedState: "ACCEPTED",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.seedID != "" {
				state := "OPEN"
				if tt.seedState != "" {
					state = tt.seedState
				}

				hs := makeOpenHandshake(tt.seedID)
				hs.State = state
				b.AddHandshakeInternal(hs)
			}

			var (
				result *organizations.Handshake
				err    error
			)

			switch tt.op {
			case "DescribeHandshake":
				result, err = b.DescribeHandshake(tt.id)
			case "AcceptHandshake":
				result, err = b.AcceptHandshake(tt.id)
			case "CancelHandshake":
				result, err = b.CancelHandshake(tt.id)
			case "DeclineHandshake":
				result, err = b.DeclineHandshake(tt.id)
			}

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, tt.id, result.ID)
			assert.Equal(t, tt.wantState, result.State)
		})
	}
}

// TestBackend_ResourcePolicyOperations tests resource policy CRUD methods.
func TestBackend_ResourcePolicyOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		content    string
		seedPolicy bool
		wantErr    bool
	}{
		{
			name:    "describe_no_policy",
			op:      "DescribeResourcePolicy",
			wantErr: true,
		},
		{
			name:    "delete_no_policy",
			op:      "DeleteResourcePolicy",
			wantErr: true,
		},
		{
			name:       "describe_existing_policy",
			op:         "DescribeResourcePolicy",
			seedPolicy: true,
			content:    `{"Version":"2012-10-17","Statement":[]}`,
		},
		{
			name:       "delete_existing_policy",
			op:         "DeleteResourcePolicy",
			seedPolicy: true,
			content:    `{"Version":"2012-10-17","Statement":[]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			if tt.seedPolicy {
				_, err := b.PutResourcePolicy(tt.content)
				require.NoError(t, err)
			}

			switch tt.op {
			case "DescribeResourcePolicy":
				rp, err := b.DescribeResourcePolicy()

				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
				require.NotNil(t, rp)
				assert.Equal(t, tt.content, rp.Content)
				assert.NotEmpty(t, rp.ID)
				assert.NotEmpty(t, rp.ARN)

			case "DeleteResourcePolicy":
				err := b.DeleteResourcePolicy()

				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)

				// After deletion, describe should fail.
				_, descErr := b.DescribeResourcePolicy()
				require.Error(t, descErr)
			}
		})
	}
}

// TestBackend_DescribeEffectivePolicy tests the DescribeEffectivePolicy backend method.
func TestBackend_DescribeEffectivePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policyType string
		targetID   string
		seedPolicy bool
		noOrg      bool
		wantErr    bool
	}{
		{
			name:       "no_policy_returns_not_found",
			policyType: "SERVICE_CONTROL_POLICY",
			wantErr:    true,
		},
		{
			name:       "effective_policy_found",
			policyType: "SERVICE_CONTROL_POLICY",
			seedPolicy: true,
		},
		{
			name:       "no_org_returns_error",
			policyType: "SERVICE_CONTROL_POLICY",
			noOrg:      true,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if !tt.noOrg {
				_, _, err := b.CreateOrganization("ALL")
				require.NoError(t, err)
			}

			var targetID string

			if tt.seedPolicy {
				// Create an account.
				status, acctErr := b.CreateAccount("ep-test", "ep@example.com", "", "", nil)
				require.NoError(t, acctErr)
				targetID = status.AccountID

				// Create a policy.
				p, pErr := b.CreatePolicy(
					"ep-scp",
					"desc",
					`{"Version":"2012-10-17","Statement":[]}`,
					tt.policyType,
					nil,
				)
				require.NoError(t, pErr)

				// Attach policy to account.
				require.NoError(t, b.AttachPolicy(p.PolicySummary.ID, targetID))
			}

			ep, err := b.DescribeEffectivePolicy(tt.policyType, targetID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, ep)
			assert.Equal(t, tt.policyType, ep.PolicyType)
			assert.NotEmpty(t, ep.PolicyContent)
			assert.NotEmpty(t, ep.PolicyID)
		})
	}
}

// TestBackend_DescribeResponsibilityTransfer tests the backend method.
func TestBackend_DescribeResponsibilityTransfer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		handshakeID string
		seed        bool
		wantErr     bool
	}{
		{
			name:        "found",
			handshakeID: "h-rt00001",
			seed:        true,
		},
		{
			name:        "not_found",
			handshakeID: "h-notfound",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.seed {
				now := time.Now()
				b.AddHandshakeInternal(&organizations.Handshake{
					ID:                  tt.handshakeID,
					ARN:                 "arn:aws:organizations::123456789012:handshake/o/transfer/" + tt.handshakeID,
					Action:              "APPROVE_ALL_FEATURES",
					State:               "OPEN",
					RequestedTimestamp:  now,
					ExpirationTimestamp: now.Add(7 * 24 * time.Hour),
				})
			}

			result, err := b.DescribeResponsibilityTransfer(tt.handshakeID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, tt.handshakeID, result.ID)
		})
	}
}
