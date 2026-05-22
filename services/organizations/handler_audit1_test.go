package organizations_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// TestAudit1_PolicyTypes tests all six supported policy types for CRUD + enable/disable.
func TestAudit1_PolicyTypes(t *testing.T) {
	t.Parallel()

	policyTypes := []string{
		"SERVICE_CONTROL_POLICY",
		"TAG_POLICY",
		"BACKUP_POLICY",
		"AISERVICES_OPT_OUT_POLICY",
		"CHATBOT_POLICY",
		"DECLARATIVE_POLICY_EC2",
	}

	for _, pt := range policyTypes {
		pt := pt

		t.Run(pt, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			p, err := b.CreatePolicy("test-"+pt, "desc", `{"Version":"2012-10-17"}`, pt, nil)
			require.NoError(t, err, "CreatePolicy")
			assert.Equal(t, pt, p.PolicySummary.Type)

			listed, err := b.ListPolicies(pt)
			require.NoError(t, err, "ListPolicies")
			assert.Len(t, listed, 1)

			_, err = b.EnablePolicyType(rootID, pt)
			require.NoError(t, err, "EnablePolicyType")

			root, err := b.DisablePolicyType(rootID, pt)
			require.NoError(t, err, "DisablePolicyType")
			assert.Empty(t, root.PolicyTypes)

			err = b.DeletePolicy(p.PolicySummary.ID)
			require.NoError(t, err, "DeletePolicy")
		})
	}
}

// TestAudit1_PolicyTypes_InvalidType tests that invalid policy types are rejected.
func TestAudit1_PolicyTypes_InvalidType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policyType string
	}{
		{name: "empty", policyType: ""},
		{name: "wrong_type", policyType: "NOT_A_REAL_TYPE"},
		{name: "lowercase", policyType: "service_control_policy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			_, err := b.CreatePolicy("p", "d", `{}`, tt.policyType, nil)
			require.Error(t, err)
		})
	}
}

// TestAudit1_EnableAllFeatures tests that EnableAllFeatures returns a Handshake.
func TestAudit1_EnableAllFeatures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		createOrg  bool
		wantAction string
		wantState  string
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

// TestAudit1_EnableAllFeatures_Backend tests the backend directly.
func TestAudit1_EnableAllFeatures_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		hasOrg    bool
		wantErr   bool
		wantState string
	}{
		{name: "creates_handshake", hasOrg: true, wantState: "OPEN"},
		{name: "no_org", hasOrg: false, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.hasOrg {
				_, _, err := b.CreateOrganization("ALL")
				require.NoError(t, err)
			}

			hs, err := b.EnableAllFeatures()

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, hs)
			assert.Equal(t, "ENABLE_ALL_FEATURES", hs.Action)
			assert.Equal(t, tt.wantState, hs.State)
			assert.NotEmpty(t, hs.ID)
			assert.NotEmpty(t, hs.ARN)
			assert.False(t, hs.ExpirationTimestamp.IsZero())
		})
	}
}

// TestAudit1_AcceptHandshake_InviteAddsAccount tests that accepting an INVITE handshake
// causes the invited account to appear in the organization.
func TestAudit1_AcceptHandshake_InviteAddsAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		targetID        string
		preExisting     bool
		wantAccountInOrg bool
	}{
		{
			name:             "new_account_added",
			targetID:         "555555555555",
			preExisting:      false,
			wantAccountInOrg: true,
		},
		{
			name:             "existing_account_not_duplicated",
			targetID:         "555555555556",
			preExisting:      true,
			wantAccountInOrg: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			if tt.preExisting {
				b.AddAccountInternal(&organizations.Account{
					ID:     tt.targetID,
					ARN:    "arn:aws:organizations::123456789012:account/o-test/" + tt.targetID,
					Name:   "pre-existing",
					Email:  tt.targetID + "@example.com",
					Status: "ACTIVE",
				})
			}

			hs, err := b.InviteAccountToOrganization(
				organizations.HandshakeParty{ID: tt.targetID, Type: "ACCOUNT"},
				"",
			)
			require.NoError(t, err)
			assert.Equal(t, "OPEN", hs.State)

			accepted, err := b.AcceptHandshake(hs.ID)
			require.NoError(t, err)
			assert.Equal(t, "ACCEPTED", accepted.State)

			if tt.wantAccountInOrg {
				acct, err := b.DescribeAccount(tt.targetID)
				require.NoError(t, err)
				assert.Equal(t, tt.targetID, acct.ID)
			}
		})
	}
}

// TestAudit1_AcceptHandshake_StateTransitions tests accept/cancel/decline state machine.
func TestAudit1_AcceptHandshake_StateTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		transition string // "accept", "cancel", "decline"
		wantState  string
		setupState string // pre-set state (empty = OPEN)
		wantErr    bool
	}{
		{name: "accept_open", transition: "accept", wantState: "ACCEPTED"},
		{name: "cancel_open", transition: "cancel", wantState: "CANCELED"},
		{name: "decline_open", transition: "decline", wantState: "DECLINED"},
		{
			name:       "accept_already_accepted",
			transition: "accept",
			setupState: "ACCEPTED",
			wantErr:    true,
		},
		{
			name:       "cancel_already_canceled",
			transition: "cancel",
			setupState: "CANCELED",
			wantErr:    true,
		},
		{
			name:       "decline_already_declined",
			transition: "decline",
			setupState: "DECLINED",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			state := "OPEN"
			if tt.setupState != "" {
				state = tt.setupState
			}

			b.AddHandshakeInternal(&organizations.Handshake{
				Action: "INVITE",
				State:  state,
				Parties: []organizations.HandshakeParty{
					{ID: "123456789012", Type: "ACCOUNT"},
				},
			})

			handshakes, err := b.ListHandshakesForOrganization("")
			require.NoError(t, err)
			require.Len(t, handshakes, 1)
			hsID := handshakes[0].ID

			var transErr error
			var result *organizations.Handshake

			switch tt.transition {
			case "accept":
				result, transErr = b.AcceptHandshake(hsID)
			case "cancel":
				result, transErr = b.CancelHandshake(hsID)
			case "decline":
				result, transErr = b.DeclineHandshake(hsID)
			}

			if tt.wantErr {
				require.Error(t, transErr)
				return
			}

			require.NoError(t, transErr)
			assert.Equal(t, tt.wantState, result.State)
		})
	}
}

// TestAudit1_HandshakeFilter_ForAccount tests ListHandshakesForAccount with ActionType filter.
func TestAudit1_HandshakeFilter_ForAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		filter      string
		wantCount   int
	}{
		{name: "no_filter_returns_all", filter: "", wantCount: 2},
		{name: "filter_invite", filter: "INVITE", wantCount: 1},
		{name: "filter_enable_all_features", filter: "ENABLE_ALL_FEATURES", wantCount: 1},
		{name: "filter_nonexistent", filter: "APPROVE_ALL_FEATURES", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			b.AddHandshakeInternal(&organizations.Handshake{Action: "INVITE", State: "OPEN"})
			b.AddHandshakeInternal(&organizations.Handshake{Action: "ENABLE_ALL_FEATURES", State: "OPEN"})

			out, err := b.ListHandshakesForAccount(tt.filter)
			require.NoError(t, err)
			assert.Len(t, out, tt.wantCount)
		})
	}
}

// TestAudit1_HandshakeFilter_ForOrganization tests ListHandshakesForOrganization with ActionType filter.
func TestAudit1_HandshakeFilter_ForOrganization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    string
		wantCount int
	}{
		{name: "no_filter_returns_all", filter: "", wantCount: 3},
		{name: "filter_invite", filter: "INVITE", wantCount: 2},
		{name: "filter_enable", filter: "ENABLE_ALL_FEATURES", wantCount: 1},
		{name: "filter_no_match", filter: "CANCELLED_THING", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			b.AddHandshakeInternal(&organizations.Handshake{Action: "INVITE", State: "OPEN"})
			b.AddHandshakeInternal(&organizations.Handshake{Action: "INVITE", State: "ACCEPTED"})
			b.AddHandshakeInternal(&organizations.Handshake{Action: "ENABLE_ALL_FEATURES", State: "OPEN"})

			out, err := b.ListHandshakesForOrganization(tt.filter)
			require.NoError(t, err)
			assert.Len(t, out, tt.wantCount)
		})
	}
}

// TestAudit1_HandshakeFilter_Handler tests the HTTP handler for handshake filter.
func TestAudit1_HandshakeFilter_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		op        string
		body      map[string]any
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

// TestAudit1_DescribeEffectivePolicy_AllPolicyTypes tests DescribeEffectivePolicy
// works for all six policy types when a policy is attached to the root.
func TestAudit1_DescribeEffectivePolicy_AllPolicyTypes(t *testing.T) {
	t.Parallel()

	policyTypes := []string{
		"SERVICE_CONTROL_POLICY",
		"TAG_POLICY",
		"BACKUP_POLICY",
		"AISERVICES_OPT_OUT_POLICY",
		"CHATBOT_POLICY",
		"DECLARATIVE_POLICY_EC2",
	}

	for _, pt := range policyTypes {
		pt := pt

		t.Run(pt, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			p, err := b.CreatePolicy("p-"+pt, "", `{"Version":"2012-10-17"}`, pt, nil)
			require.NoError(t, err)

			// Attach to root so hierarchy walk finds it from any child.
			err = b.AttachPolicy(p.PolicySummary.ID, rootID)
			require.NoError(t, err)

			// Look up effective policy from root directly.
			ep, err := b.DescribeEffectivePolicy(pt, rootID)
			require.NoError(t, err)
			assert.Equal(t, pt, ep.PolicyType)
			assert.Equal(t, rootID, ep.TargetID)
		})
	}
}

// TestAudit1_ListAccountsWithInvalidEffectivePolicy_AllTypes tests
// all six policy types are accepted.
func TestAudit1_ListAccountsWithInvalidEffectivePolicy_AllTypes(t *testing.T) {
	t.Parallel()

	policyTypes := []string{
		"SERVICE_CONTROL_POLICY",
		"TAG_POLICY",
		"BACKUP_POLICY",
		"AISERVICES_OPT_OUT_POLICY",
		"CHATBOT_POLICY",
		"DECLARATIVE_POLICY_EC2",
	}

	for _, pt := range policyTypes {
		pt := pt

		t.Run(pt, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			accounts, err := b.ListAccountsWithInvalidEffectivePolicy(pt)
			require.NoError(t, err)
			assert.Empty(t, accounts)
		})
	}
}

// TestAudit1_ListEffectivePolicyValidationErrors_AllTypes tests all six policy types.
func TestAudit1_ListEffectivePolicyValidationErrors_AllTypes(t *testing.T) {
	t.Parallel()

	policyTypes := []string{
		"SERVICE_CONTROL_POLICY",
		"TAG_POLICY",
		"BACKUP_POLICY",
		"AISERVICES_OPT_OUT_POLICY",
		"CHATBOT_POLICY",
		"DECLARATIVE_POLICY_EC2",
	}

	for _, pt := range policyTypes {
		pt := pt

		t.Run(pt, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			errs, err := b.ListEffectivePolicyValidationErrors(pt, "")
			require.NoError(t, err)
			assert.Empty(t, errs)
		})
	}
}

// TestAudit1_EnableAllFeatures_HandshakeInList tests that the ENABLE_ALL_FEATURES
// handshake created by EnableAllFeatures appears in list operations.
func TestAudit1_EnableAllFeatures_HandshakeInList(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	hs, err := b.EnableAllFeatures()
	require.NoError(t, err)

	allHs, err := b.ListHandshakesForOrganization("")
	require.NoError(t, err)

	found := false
	for _, h := range allHs {
		if h.ID == hs.ID {
			found = true
			assert.Equal(t, "ENABLE_ALL_FEATURES", h.Action)
			assert.Equal(t, "OPEN", h.State)
		}
	}

	assert.True(t, found, "ENABLE_ALL_FEATURES handshake must appear in ListHandshakesForOrganization")
}

// TestAudit1_PolicyTypes_Handler tests the HTTP handler accepts all six policy types.
func TestAudit1_PolicyTypes_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policyType string
		wantStatus int
	}{
		{name: "SCP", policyType: "SERVICE_CONTROL_POLICY", wantStatus: http.StatusOK},
		{name: "TAG", policyType: "TAG_POLICY", wantStatus: http.StatusOK},
		{name: "BACKUP", policyType: "BACKUP_POLICY", wantStatus: http.StatusOK},
		{name: "AISERVICES", policyType: "AISERVICES_OPT_OUT_POLICY", wantStatus: http.StatusOK},
		{name: "CHATBOT", policyType: "CHATBOT_POLICY", wantStatus: http.StatusOK},
		{name: "DECLARATIVE_EC2", policyType: "DECLARATIVE_POLICY_EC2", wantStatus: http.StatusOK},
		{name: "invalid", policyType: "INVALID_TYPE", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			rec := doRequest(t, h, "CreatePolicy", map[string]any{
				"Name":        "test-policy",
				"Description": "desc",
				"Content":     `{"Version":"2012-10-17"}`,
				"Type":        tt.policyType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
