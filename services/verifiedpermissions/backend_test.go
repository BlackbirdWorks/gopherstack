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

			ps, err := b.CreatePolicyStore(tt.description, nil, "OFF", "")
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
				_, err := b.CreatePolicyStore("desc", nil, "OFF", "")
				require.NoError(t, err)
			}

			stores, _ := b.ListPolicyStores("", 0)
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

				ps, err := b.CreatePolicyStore("original", nil, "OFF", "")
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

			ps, err := b.UpdatePolicyStore(id, tt.newDesc, "", "")
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

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "")
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

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "")
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

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "")
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

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "")
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

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "")
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

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "")
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

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numStores int
	}{
		{name: "reset empty backend", numStores: 0},
		{name: "reset with data", numStores: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			for range tt.numStores {
				_, err := b.CreatePolicyStore("desc", nil, "OFF", "")
				require.NoError(t, err)
			}

			stores, _ := b.ListPolicyStores("", 0)
			assert.Len(t, stores, tt.numStores)

			b.Reset()

			stores, _ = b.ListPolicyStores("", 0)
			assert.Empty(t, stores)
		})
	}
}

func TestBackend_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*testing.T, *verifiedpermissions.InMemoryBackend) string
		tags    map[string]string
		name    string
		wantErr bool
	}{
		{
			name: "tag existing policy store",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) string {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "")
				require.NoError(t, err)

				return ps.Arn
			},
			tags:    map[string]string{"env": "prod", "team": "platform"},
			wantErr: false,
		},
		{
			name: "tag non-existent resource",
			setup: func(_ *testing.T, _ *verifiedpermissions.InMemoryBackend) string {
				return "arn:aws:verifiedpermissions::123456789012:policy-store/nonexistent"
			},
			tags:    map[string]string{"key": "value"},
			wantErr: true,
		},
		{
			name: "add tags to already tagged store",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) string {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", map[string]string{"existing": "tag"}, "OFF", "")
				require.NoError(t, err)

				return ps.Arn
			},
			tags:    map[string]string{"new": "tag"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			arn := tt.setup(t, b)

			err := b.TagResource(arn, tt.tags)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			tags, err := b.ListTagsForResource(arn)
			require.NoError(t, err)

			for k, v := range tt.tags {
				assert.Equal(t, v, tags[k])
			}
		})
	}
}

func TestBackend_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*testing.T, *verifiedpermissions.InMemoryBackend) string
		name    string
		tagKeys []string
		wantErr bool
	}{
		{
			name: "untag existing resource",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) string {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", map[string]string{"env": "prod", "team": "platform"}, "OFF", "")
				require.NoError(t, err)

				return ps.Arn
			},
			tagKeys: []string{"env"},
			wantErr: false,
		},
		{
			name: "untag non-existent resource",
			setup: func(_ *testing.T, _ *verifiedpermissions.InMemoryBackend) string {
				return "arn:aws:verifiedpermissions::123456789012:policy-store/nonexistent"
			},
			tagKeys: []string{"key"},
			wantErr: true,
		},
		{
			name: "untag with empty keys removes nothing",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) string {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", map[string]string{"env": "prod"}, "OFF", "")
				require.NoError(t, err)

				return ps.Arn
			},
			tagKeys: []string{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			arn := tt.setup(t, b)

			err := b.UntagResource(arn, tt.tagKeys)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if len(tt.tagKeys) > 0 {
				tags, tagsErr := b.ListTagsForResource(arn)
				require.NoError(t, tagsErr)

				for _, k := range tt.tagKeys {
					_, exists := tags[k]
					assert.False(t, exists, "tag %q should have been removed", k)
				}
			}
		})
	}
}

func TestBackend_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.InMemoryBackend) string
		wantTags map[string]string
		name     string
		wantErr  bool
	}{
		{
			name: "list tags for existing resource with tags",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) string {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", map[string]string{"env": "prod"}, "OFF", "")
				require.NoError(t, err)

				return ps.Arn
			},
			wantTags: map[string]string{"env": "prod"},
			wantErr:  false,
		},
		{
			name: "list tags for resource without tags",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) string {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "")
				require.NoError(t, err)

				return ps.Arn
			},
			wantTags: map[string]string{},
			wantErr:  false,
		},
		{
			name: "list tags for non-existent resource",
			setup: func(_ *testing.T, _ *verifiedpermissions.InMemoryBackend) string {
				return "arn:aws:verifiedpermissions::123456789012:policy-store/nonexistent"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			arn := tt.setup(t, b)

			tags, err := b.ListTagsForResource(arn)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			for k, v := range tt.wantTags {
				assert.Equal(t, v, tags[k])
			}
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

func TestBackend_CreatePolicyTemplate_NonExistentStore(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.CreatePolicyTemplate("nonexistent-store", "desc", "permit(principal == ?principal, action, resource);")
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestBackend_GetPolicyTemplate_NonExistentInExistingStore(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	ps, err := b.CreatePolicyStore("desc", nil, "OFF", "")
	require.NoError(t, err)

	_, err = b.GetPolicyTemplate(ps.PolicyStoreID, "nonexistent-template")
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestBackend_UpdatePolicyTemplate_NonExistentStore(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.UpdatePolicyTemplate("nonexistent-store", "nonexistent-template", "desc", "stmt")
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

func TestBackend_Snapshot_Restore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numStores int
	}{
		{name: "empty backend", numStores: 0},
		{name: "with stores and policies", numStores: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			var storeIDs []string

			for range tt.numStores {
				ps, err := b.CreatePolicyStore("desc", map[string]string{"env": "test"}, "OFF", "")
				require.NoError(t, err)

				_, err = b.CreatePolicy(
					ps.PolicyStoreID,
					verifiedpermissions.CreatePolicyParams{
						PolicyType: "STATIC",
						Statement:  "permit(principal, action, resource);",
					},
				)
				require.NoError(t, err)

				_, err = b.CreatePolicyTemplate(
					ps.PolicyStoreID, "tpl", "permit(principal == ?principal, action, resource);",
				)
				require.NoError(t, err)

				storeIDs = append(storeIDs, ps.PolicyStoreID)
			}

			snap := b.Snapshot()
			require.NotNil(t, snap)

			b2 := newTestBackend()
			require.NoError(t, b2.Restore(snap))

			stores2, _ := b2.ListPolicyStores("", 0)
			assert.Len(t, stores2, tt.numStores)

			for _, id := range storeIDs {
				ps, err := b2.GetPolicyStore(id)
				require.NoError(t, err)
				assert.Equal(t, id, ps.PolicyStoreID)

				policies, _, err := b2.ListPolicies(id, verifiedpermissions.ListPoliciesFilter{}, "", 0)
				require.NoError(t, err)
				assert.Len(t, policies, 1)

				templates, _, err := b2.ListPolicyTemplates(id, "", 0)
				require.NoError(t, err)
				assert.Len(t, templates, 1)
			}
		})
	}
}

func TestBackend_Restore_InvalidJSON(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

func TestBackend_CreatePolicyStore_WithTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
	}{
		{
			name: "with tags",
			tags: map[string]string{"env": "prod", "team": "platform"},
		},
		{
			name: "with nil tags",
			tags: nil,
		},
		{
			name: "with empty tags",
			tags: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			ps, err := b.CreatePolicyStore("desc", tt.tags, "OFF", "")
			require.NoError(t, err)
			assert.NotEmpty(t, ps.PolicyStoreID)
			assert.NotEmpty(t, ps.Arn)

			for k, v := range tt.tags {
				assert.Equal(t, v, ps.Tags[k])
			}
		})
	}
}
