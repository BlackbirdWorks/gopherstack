package verifiedpermissions_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_Policy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*testing.T, *verifiedpermissions.InMemoryBackend) (string, string)
		name    string
		wantErr bool
	}{
		{
			name: "create and get",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) (string, string) {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "")
				require.NoError(t, err)

				p, err := b.CreatePolicy(
					ps.PolicyStoreID,
					verifiedpermissions.CreatePolicyParams{
						PolicyType: "STATIC",
						Statement:  "permit(principal, action, resource);",
					},
				)
				require.NoError(t, err)

				return ps.PolicyStoreID, p.PolicyID
			},
			wantErr: false,
		},
		{
			name: "get from non-existent store",
			setup: func(t *testing.T, _ *verifiedpermissions.InMemoryBackend) (string, string) {
				t.Helper()

				return "nonexistent-store", "nonexistent-policy"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			storeID, policyID := tt.setup(t, b)

			p, err := b.GetPolicy(storeID, policyID)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, policyID, p.PolicyID)
			assert.Equal(t, storeID, p.PolicyStoreID)
			assert.Equal(t, "STATIC", p.PolicyType)
		})
	}
}

func TestBackend_ListPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *verifiedpermissions.InMemoryBackend) string
		name        string
		numPolicies int
		wantErr     bool
	}{
		{
			name: "list multiple policies",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) string {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "")
				require.NoError(t, err)

				return ps.PolicyStoreID
			},
			numPolicies: 2,
			wantErr:     false,
		},
		{
			name: "list from non-existent store",
			setup: func(_ *testing.T, _ *verifiedpermissions.InMemoryBackend) string {
				return "nonexistent"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			storeID := tt.setup(t, b)

			for range tt.numPolicies {
				_, err := b.CreatePolicy(
					storeID,
					verifiedpermissions.CreatePolicyParams{
						PolicyType: "STATIC",
						Statement:  "permit(principal, action, resource);",
					},
				)
				require.NoError(t, err)
			}

			policies, _, err := b.ListPolicies(storeID, verifiedpermissions.ListPoliciesFilter{}, "", 0)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, policies, tt.numPolicies)
		})
	}
}

func TestBackend_UpdatePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*testing.T, *verifiedpermissions.InMemoryBackend) (string, string)
		name    string
		newStmt string
		wantErr bool
	}{
		{
			name: "update existing policy",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) (string, string) {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "")
				require.NoError(t, err)

				p, err := b.CreatePolicy(
					ps.PolicyStoreID,
					verifiedpermissions.CreatePolicyParams{
						PolicyType: "STATIC",
						Statement:  "permit(principal, action, resource);",
					},
				)
				require.NoError(t, err)

				return ps.PolicyStoreID, p.PolicyID
			},
			newStmt: "forbid(principal, action, resource);",
			wantErr: false,
		},
		{
			name: "update non-existent policy",
			setup: func(_ *testing.T, _ *verifiedpermissions.InMemoryBackend) (string, string) {
				return "nonexistent-store", "nonexistent-policy"
			},
			newStmt: "forbid(principal, action, resource);",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			storeID, policyID := tt.setup(t, b)

			p, err := b.UpdatePolicy(storeID, policyID, verifiedpermissions.UpdatePolicyParams{Statement: tt.newStmt})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.newStmt, p.Statement)
		})
	}
}

func TestBackend_DeletePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*testing.T, *verifiedpermissions.InMemoryBackend) (string, string)
		name    string
		wantErr bool
	}{
		{
			name: "delete existing",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) (string, string) {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "")
				require.NoError(t, err)

				p, err := b.CreatePolicy(
					ps.PolicyStoreID,
					verifiedpermissions.CreatePolicyParams{
						PolicyType: "STATIC",
						Statement:  "permit(principal, action, resource);",
					},
				)
				require.NoError(t, err)

				return ps.PolicyStoreID, p.PolicyID
			},
			wantErr: false,
		},
		{
			name: "delete non-existent policy",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) (string, string) {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "")
				require.NoError(t, err)

				return ps.PolicyStoreID, "nonexistent-policy"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			storeID, policyID := tt.setup(t, b)

			err := b.DeletePolicy(storeID, policyID)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			_, err = b.GetPolicy(storeID, policyID)
			require.Error(t, err)
		})
	}
}

func TestBackend_CreatePolicy_NonExistentStore(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.CreatePolicy(
		"nonexistent-store",
		verifiedpermissions.CreatePolicyParams{PolicyType: "STATIC", Statement: "permit(principal, action, resource);"},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestBackend_GetPolicy_NonExistentPolicyInExistingStore(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	ps, err := b.CreatePolicyStore("desc", nil, "OFF", "")
	require.NoError(t, err)

	_, err = b.GetPolicy(ps.PolicyStoreID, "nonexistent-policy")
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestBackend_DeletePolicy_NonExistentStore(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	err := b.DeletePolicy("nonexistent-store", "nonexistent-policy")
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestBackend_UpdatePolicy_NonExistentPolicy(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	ps, err := b.CreatePolicyStore("desc", nil, "OFF", "")
	require.NoError(t, err)

	_, err = b.UpdatePolicy(
		ps.PolicyStoreID,
		"nonexistent-policy",
		verifiedpermissions.UpdatePolicyParams{Statement: "forbid(principal, action, resource);"},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestBackend_BatchGetPolicy_EmptyArrays(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	ps := seedPolicyStore(t, b, "batch store")

	result := b.BatchGetPolicy([]verifiedpermissions.BatchGetPolicyItem{
		{PolicyStoreID: ps.PolicyStoreID, PolicyID: "nonexistent"},
	})

	assert.Empty(t, result.Results)
	assert.Len(t, result.Errors, 1)
}
