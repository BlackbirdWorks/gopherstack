package verifiedpermissions_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
)

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

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "", "")
				require.NoError(t, err)

				pt, err := b.CreatePolicyTemplate(
					ps.PolicyStoreID,
					"My Template",
					"permit(principal == ?principal, action, resource);", "", "",
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

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "", "")
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
					"permit(principal == ?principal, action, resource);", "", "",
				)
				require.NoError(t, err)
			}

			templates, _, err := b.ListPolicyTemplates(storeID, "", 0)
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

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "", "")
				require.NoError(t, err)

				pt, err := b.CreatePolicyTemplate(
					ps.PolicyStoreID,
					"original",
					"permit(principal == ?principal, action, resource);", "", "",
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

			pt, err := b.UpdatePolicyTemplate(storeID, templateID, tt.newDesc, tt.newStmt, "")
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

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "", "")
				require.NoError(t, err)

				pt, err := b.CreatePolicyTemplate(
					ps.PolicyStoreID,
					"desc",
					"permit(principal == ?principal, action, resource);", "", "",
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

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "", "")
				require.NoError(t, err)

				return ps.PolicyStoreID, "nonexistent-template"
			},
			wantErr: true,
		},
		{
			name: "delete from non-existent store",
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

func TestBackend_CreatePolicyTemplate_NonExistentStore(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.CreatePolicyTemplate(
		"nonexistent-store",
		"desc",
		"permit(principal == ?principal, action, resource);",
		"",
		"",
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestBackend_GetPolicyTemplate_NonExistentInExistingStore(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	ps, err := b.CreatePolicyStore("desc", nil, "OFF", "", "")
	require.NoError(t, err)

	_, err = b.GetPolicyTemplate(ps.PolicyStoreID, "nonexistent-template")
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestBackend_UpdatePolicyTemplate_NonExistentStore(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.UpdatePolicyTemplate("nonexistent-store", "nonexistent-template", "desc", "stmt", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

// TestBackend_DeletePolicyTemplate_CascadesToLinkedPolicies locks in the
// real SDK's documented DeletePolicyTemplate behavior ("This operation also
// deletes any policies that were created from the specified policy
// template"): every TEMPLATE_LINKED policy referencing the deleted template
// must itself be deleted (ARN index, tags, and the policy row), while a
// STATIC policy in the same store is left untouched.
func TestBackend_DeletePolicyTemplate_CascadesToLinkedPolicies(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	ps := seedPolicyStore(t, b, "cascade store")

	tmpl, err := b.CreatePolicyTemplate(
		ps.PolicyStoreID, "tmpl", `permit(principal == ?principal, action, resource);`, "", "",
	)
	require.NoError(t, err)

	linked, err := b.CreatePolicy(ps.PolicyStoreID, verifiedpermissions.CreatePolicyParams{
		PolicyType:          "TEMPLATE_LINKED",
		PolicyTemplateID:    tmpl.PolicyTemplateID,
		PrincipalEntityType: "User",
		PrincipalEntityID:   "alice",
	})
	require.NoError(t, err)

	static, err := b.CreatePolicy(ps.PolicyStoreID, verifiedpermissions.CreatePolicyParams{
		PolicyType: "STATIC",
		Statement:  "permit(principal, action, resource);",
	})
	require.NoError(t, err)

	require.NoError(t, b.DeletePolicyTemplate(ps.PolicyStoreID, tmpl.PolicyTemplateID))

	_, err = b.GetPolicy(ps.PolicyStoreID, linked.PolicyID)
	require.ErrorIs(t, err, awserr.ErrNotFound, "template-linked policy must be cascade-deleted with its template")

	got, err := b.GetPolicy(ps.PolicyStoreID, static.PolicyID)
	require.NoError(t, err, "static policy in the same store must survive the template deletion")
	assert.Equal(t, static.PolicyID, got.PolicyID)

	policies, _, err := b.ListPolicies(ps.PolicyStoreID, verifiedpermissions.ListPoliciesFilter{}, "", 0)
	require.NoError(t, err)
	assert.Len(t, policies, 1, "only the static policy should remain")
}
