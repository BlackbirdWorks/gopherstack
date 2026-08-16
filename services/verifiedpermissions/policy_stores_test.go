package verifiedpermissions_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
)

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

			ps, err := b.CreatePolicyStore(tt.description, nil, "OFF", "", "")
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
				_, err := b.CreatePolicyStore("desc", nil, "OFF", "", "")
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

				ps, err := b.CreatePolicyStore("original", nil, "OFF", "", "")
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

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "", "")
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
			ps, err := b.CreatePolicyStore("desc", tt.tags, "OFF", "", "")
			require.NoError(t, err)
			assert.NotEmpty(t, ps.PolicyStoreID)
			assert.NotEmpty(t, ps.Arn)

			for k, v := range tt.tags {
				assert.Equal(t, v, ps.Tags[k])
			}
		})
	}
}

func TestBackend_DeletePolicyStore_ARNIndexCleaned(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	ps := seedPolicyStore(t, b, "to delete")

	require.Equal(t, 1, verifiedpermissions.ARNIndexSize(b))

	err := b.DeletePolicyStore(ps.PolicyStoreID)
	require.NoError(t, err)

	assert.Equal(t, 0, verifiedpermissions.ARNIndexSize(b))
}
