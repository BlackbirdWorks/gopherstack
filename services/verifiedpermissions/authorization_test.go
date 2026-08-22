package verifiedpermissions_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
)

func TestBackend_IsAuthorized(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		policyStoreID string
		wantDecision  string
		wantErr       bool
	}{
		{
			name:         "allow on existing store",
			wantErr:      false,
			wantDecision: "DENY",
		},
		{
			name:          "not found on missing store",
			policyStoreID: "missing-store",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			ps := seedPolicyStore(t, b, "auth store")

			id := ps.PolicyStoreID
			if tt.policyStoreID != "" {
				id = tt.policyStoreID
			}

			decision, err := b.IsAuthorized(id, verifiedpermissions.AuthorizationRequest{
				PrincipalEntityType: "User",
				PrincipalEntityID:   "alice",
				ActionType:          "Action",
				ActionID:            "view",
				ResourceEntityType:  "Document",
				ResourceEntityID:    "doc1",
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantDecision, decision.Decision)
			assert.NotNil(t, decision.DeterminingPolicies)
			assert.NotNil(t, decision.Errors)
		})
	}
}

func TestBackend_IsAuthorizedWithToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		policyStoreID string
		wantDecision  string
		wantErr       bool
	}{
		{
			name:         "allow on existing store",
			wantErr:      false,
			wantDecision: "DENY",
		},
		{
			name:          "not found on missing store",
			policyStoreID: "missing-store",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			ps := seedPolicyStore(t, b, "token auth store")

			id := ps.PolicyStoreID
			if tt.policyStoreID != "" {
				id = tt.policyStoreID
			}

			decision, err := b.IsAuthorizedWithToken(id, verifiedpermissions.AuthorizationRequest{
				ActionType: "Action",
				ActionID:   "view",
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantDecision, decision.Decision)
			assert.NotNil(t, decision.DeterminingPolicies)
			assert.NotNil(t, decision.Errors)
		})
	}
}

func TestBackend_BatchIsAuthorizedOutputFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		policyStoreID string
		requests      int
		wantErr       bool
	}{
		{
			name:     "single request",
			requests: 1,
			wantErr:  false,
		},
		{
			name:     "empty requests",
			requests: 0,
			wantErr:  false,
		},
		{
			name:          "unknown policy store",
			policyStoreID: "missing",
			requests:      1,
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			ps := seedPolicyStore(t, b, "batch store")

			psID := ps.PolicyStoreID
			if tt.policyStoreID != "" {
				psID = tt.policyStoreID
			}

			reqs := make([]verifiedpermissions.AuthorizationRequest, tt.requests)
			for i := range reqs {
				reqs[i] = verifiedpermissions.AuthorizationRequest{ActionType: "Action", ActionID: "view"}
			}

			decisions, err := b.BatchIsAuthorized(psID, reqs)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, decisions, tt.requests)

			for _, d := range decisions {
				assert.Contains(t, []string{"ALLOW", "DENY"}, d.Decision)
				assert.NotNil(t, d.DeterminingPolicies)
				assert.NotNil(t, d.Errors)
			}
		})
	}
}

func TestBackend_BatchIsAuthorizedWithToken_OutputFields(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	ps := seedPolicyStore(t, b, "token batch store")

	decisions, err := b.BatchIsAuthorizedWithToken(ps.PolicyStoreID, []verifiedpermissions.AuthorizationRequest{
		{ActionType: "Action", ActionID: "view"},
	})
	require.NoError(t, err)
	require.Len(t, decisions, 1)

	assert.Contains(t, []string{"ALLOW", "DENY"}, decisions[0].Decision)
	assert.NotNil(t, decisions[0].DeterminingPolicies)
	assert.NotNil(t, decisions[0].Errors)
}

// TestBackend_IsAuthorized_TemplateLinkedPolicy locks in the fix for a
// critical bug: buildCedarPolicySet used to skip every TEMPLATE_LINKED
// policy entirely (it only compiled STATIC policies into the Cedar
// PolicySet), so a template-linked permit policy could never actually ALLOW
// a request -- the core value proposition of a policy template. The policy
// store here has ONLY a template-linked policy (no static policies at all),
// so an ALLOW decision is only reachable if the template-linked policy was
// instantiated (its ?principal/?resource slots substituted) and included in
// evaluation.
func TestBackend_IsAuthorized_TemplateLinkedPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		reqPrincipalID string
		reqResourceID  string
		wantDecision   string
	}{
		{
			name:           "request matches the bound principal/resource: ALLOW",
			reqPrincipalID: "alice",
			reqResourceID:  "doc1",
			wantDecision:   "ALLOW",
		},
		{
			name:           "request for a different principal: DENY",
			reqPrincipalID: "mallory",
			reqResourceID:  "doc1",
			wantDecision:   "DENY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			ps := seedPolicyStore(t, b, "template-linked auth store")

			tmpl, err := b.CreatePolicyTemplate(
				ps.PolicyStoreID, "tmpl", `permit(principal == ?principal, action, resource == ?resource);`, "", "",
			)
			require.NoError(t, err)

			_, err = b.CreatePolicy(ps.PolicyStoreID, verifiedpermissions.CreatePolicyParams{
				PolicyType:          "TEMPLATE_LINKED",
				PolicyTemplateID:    tmpl.PolicyTemplateID,
				PrincipalEntityType: "User",
				PrincipalEntityID:   "alice",
				ResourceEntityType:  "Document",
				ResourceEntityID:    "doc1",
			})
			require.NoError(t, err)

			decision, err := b.IsAuthorized(ps.PolicyStoreID, verifiedpermissions.AuthorizationRequest{
				PrincipalEntityType: "User",
				PrincipalEntityID:   tt.reqPrincipalID,
				ActionType:          "Action",
				ActionID:            "view",
				ResourceEntityType:  "Document",
				ResourceEntityID:    tt.reqResourceID,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantDecision, decision.Decision)

			if tt.wantDecision == "ALLOW" {
				assert.Contains(t, decision.DeterminingPolicies, decision.DeterminingPolicies[0])
				assert.NotEmpty(t, decision.DeterminingPolicies)
			}
		})
	}
}

// TestBackend_IsAuthorized_TemplateLinkedPolicy_UnboundOptionalSlot verifies
// that a template-linked policy which only binds a principal (leaving the
// template's unconstrained resource scope alone) still evaluates and
// participates in authorization -- instantiateTemplate must not require
// every slot to be bound, only the ones the template actually references.
func TestBackend_IsAuthorized_TemplateLinkedPolicy_UnboundOptionalSlot(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	ps := seedPolicyStore(t, b, "unbound slot store")

	tmpl, err := b.CreatePolicyTemplate(
		ps.PolicyStoreID, "tmpl", `permit(principal == ?principal, action, resource);`, "", "",
	)
	require.NoError(t, err)

	_, err = b.CreatePolicy(ps.PolicyStoreID, verifiedpermissions.CreatePolicyParams{
		PolicyType:          "TEMPLATE_LINKED",
		PolicyTemplateID:    tmpl.PolicyTemplateID,
		PrincipalEntityType: "User",
		PrincipalEntityID:   "alice",
	})
	require.NoError(t, err)

	decision, err := b.IsAuthorized(ps.PolicyStoreID, verifiedpermissions.AuthorizationRequest{
		PrincipalEntityType: "User",
		PrincipalEntityID:   "alice",
		ActionType:          "Action",
		ActionID:            "view",
		ResourceEntityType:  "Document",
		ResourceEntityID:    "any-doc",
	})
	require.NoError(t, err)
	assert.Equal(t, "ALLOW", decision.Decision)
}
