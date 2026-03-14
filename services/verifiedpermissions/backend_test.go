package verifiedpermissions_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
)

func newTestBackend() *verifiedpermissions.InMemoryBackend {
	return verifiedpermissions.NewInMemoryBackend("123456789012", "us-east-1")
}

func TestBackend_PolicyStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *verifiedpermissions.InMemoryBackend) string
		name         string
		description  string
		wantErr      bool
		wantNotFound bool
	}{
		{
			name:        "create and get",
			description: "My test store",
			wantErr:     false,
		},
		{
			name:         "get non-existent",
			wantErr:      true,
			wantNotFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.wantNotFound {
				_, err := b.GetPolicyStore("nonexistent-id")
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

			ps, err := b.CreatePolicyStore(tt.description, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, ps.PolicyStoreID)
			assert.Equal(t, tt.description, ps.Description)
			assert.NotEmpty(t, ps.Arn)

			got, err := b.GetPolicyStore(ps.PolicyStoreID)
			require.NoError(t, err)
			assert.Equal(t, ps.PolicyStoreID, got.PolicyStoreID)
			assert.Equal(t, tt.description, got.Description)
		})
	}
}

func TestBackend_ListPolicyStores(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numStores int
	}{
		{
			name:      "empty list",
			numStores: 0,
		},
		{
			name:      "multiple stores",
			numStores: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			for range tt.numStores {
				_, err := b.CreatePolicyStore("desc", nil)
				require.NoError(t, err)
			}

			stores := b.ListPolicyStores()
			assert.Len(t, stores, tt.numStores)
		})
	}
}

func TestBackend_UpdatePolicyStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(*testing.T, *verifiedpermissions.InMemoryBackend) string
		name          string
		newDesc       string
		policyStoreID string
		wantErr       bool
	}{
		{
			name: "update existing",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) string {
				t.Helper()

				ps, err := b.CreatePolicyStore("original", nil)
				require.NoError(t, err)

				return ps.PolicyStoreID
			},
			newDesc: "updated description",
			wantErr: false,
		},
		{
			name:          "update non-existent",
			policyStoreID: "nonexistent-id",
			newDesc:       "desc",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			id := tt.policyStoreID

			if tt.setup != nil {
				id = tt.setup(t, b)
			}

			ps, err := b.UpdatePolicyStore(id, tt.newDesc)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.newDesc, ps.Description)
		})
	}
}

func TestBackend_DeletePolicyStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(*testing.T, *verifiedpermissions.InMemoryBackend) string
		name          string
		policyStoreID string
		wantErr       bool
	}{
		{
			name: "delete existing",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) string {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", nil)
				require.NoError(t, err)

				return ps.PolicyStoreID
			},
			wantErr: false,
		},
		{
			name:          "delete non-existent",
			policyStoreID: "nonexistent-id",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			id := tt.policyStoreID

			if tt.setup != nil {
				id = tt.setup(t, b)
			}

			err := b.DeletePolicyStore(id)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			_, err = b.GetPolicyStore(id)
			require.Error(t, err)
		})
	}
}

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

				ps, err := b.CreatePolicyStore("desc", nil)
				require.NoError(t, err)

				p, err := b.CreatePolicy(ps.PolicyStoreID, "STATIC", "permit(principal, action, resource);")
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

				ps, err := b.CreatePolicyStore("desc", nil)
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
				_, err := b.CreatePolicy(storeID, "STATIC", "permit(principal, action, resource);")
				require.NoError(t, err)
			}

			policies, err := b.ListPolicies(storeID)
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

				ps, err := b.CreatePolicyStore("desc", nil)
				require.NoError(t, err)

				p, err := b.CreatePolicy(ps.PolicyStoreID, "STATIC", "permit(principal, action, resource);")
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

			p, err := b.UpdatePolicy(storeID, policyID, tt.newStmt)
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

				ps, err := b.CreatePolicyStore("desc", nil)
				require.NoError(t, err)

				p, err := b.CreatePolicy(ps.PolicyStoreID, "STATIC", "permit(principal, action, resource);")
				require.NoError(t, err)

				return ps.PolicyStoreID, p.PolicyID
			},
			wantErr: false,
		},
		{
			name: "delete non-existent policy",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) (string, string) {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", nil)
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

func TestBackend_PolicyTemplate(t *testing.T) {
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

				ps, err := b.CreatePolicyStore("desc", nil)
				require.NoError(t, err)

				pt, err := b.CreatePolicyTemplate(
					ps.PolicyStoreID,
					"My Template",
					"permit(principal == ?principal, action, resource);",
				)
				require.NoError(t, err)

				return ps.PolicyStoreID, pt.PolicyTemplateID
			},
			wantErr: false,
		},
		{
			name: "get non-existent template",
			setup: func(_ *testing.T, _ *verifiedpermissions.InMemoryBackend) (string, string) {
				return "nonexistent-store", "nonexistent-template"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			storeID, templateID := tt.setup(t, b)

			pt, err := b.GetPolicyTemplate(storeID, templateID)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, templateID, pt.PolicyTemplateID)
			assert.Equal(t, storeID, pt.PolicyStoreID)
			assert.Equal(t, "My Template", pt.Description)
		})
	}
}

func TestBackend_ListPolicyTemplates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *verifiedpermissions.InMemoryBackend) string
		name         string
		numTemplates int
		wantErr      bool
	}{
		{
			name: "list multiple templates",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) string {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", nil)
				require.NoError(t, err)

				return ps.PolicyStoreID
			},
			numTemplates: 2,
			wantErr:      false,
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

			for i := range tt.numTemplates {
				_, err := b.CreatePolicyTemplate(
					storeID,
					fmt.Sprintf("template %d", i),
					"permit(principal == ?principal, action, resource);",
				)
				require.NoError(t, err)
			}

			templates, err := b.ListPolicyTemplates(storeID)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, templates, tt.numTemplates)
		})
	}
}

func TestBackend_UpdatePolicyTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*testing.T, *verifiedpermissions.InMemoryBackend) (string, string)
		name    string
		newDesc string
		newStmt string
		wantErr bool
	}{
		{
			name: "update existing template",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) (string, string) {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", nil)
				require.NoError(t, err)

				pt, err := b.CreatePolicyTemplate(
					ps.PolicyStoreID,
					"original",
					"permit(principal == ?principal, action, resource);",
				)
				require.NoError(t, err)

				return ps.PolicyStoreID, pt.PolicyTemplateID
			},
			newDesc: "updated",
			newStmt: "forbid(principal == ?principal, action, resource);",
			wantErr: false,
		},
		{
			name: "update non-existent template",
			setup: func(_ *testing.T, _ *verifiedpermissions.InMemoryBackend) (string, string) {
				return "nonexistent-store", "nonexistent-template"
			},
			newDesc: "updated",
			newStmt: "forbid(principal == ?principal, action, resource);",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			storeID, templateID := tt.setup(t, b)

			pt, err := b.UpdatePolicyTemplate(storeID, templateID, tt.newDesc, tt.newStmt)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.newDesc, pt.Description)
			assert.Equal(t, tt.newStmt, pt.Statement)
		})
	}
}

func TestBackend_DeletePolicyTemplate(t *testing.T) {
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

				ps, err := b.CreatePolicyStore("desc", nil)
				require.NoError(t, err)

				pt, err := b.CreatePolicyTemplate(
					ps.PolicyStoreID,
					"desc",
					"permit(principal == ?principal, action, resource);",
				)
				require.NoError(t, err)

				return ps.PolicyStoreID, pt.PolicyTemplateID
			},
			wantErr: false,
		},
		{
			name: "delete non-existent template",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) (string, string) {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", nil)
				require.NoError(t, err)

				return ps.PolicyStoreID, "nonexistent-template"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			storeID, templateID := tt.setup(t, b)

			err := b.DeletePolicyTemplate(storeID, templateID)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			_, err = b.GetPolicyTemplate(storeID, templateID)
			require.Error(t, err)
		})
	}
}
