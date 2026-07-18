package verifiedpermissions_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
