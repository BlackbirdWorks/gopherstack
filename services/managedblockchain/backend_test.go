package managedblockchain_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "000000000000"
)

func newBackend() *managedblockchain.InMemoryBackend {
	return managedblockchain.NewInMemoryBackend()
}

func TestInMemoryBackend_CreateNetwork(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errSentinel error
		name        string
		networkName string
		memberName  string
		wantErr     bool
	}{
		{
			name:        "creates network and first member",
			networkName: "my-network",
			memberName:  "my-member",
			wantErr:     false,
		},
		{
			name:        "duplicate name returns already exists",
			networkName: "dup-network",
			memberName:  "first-member",
			wantErr:     true,
			errSentinel: awserr.ErrAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			if errors.Is(tt.errSentinel, awserr.ErrAlreadyExists) {
				_, _, err := b.CreateNetwork(
					testRegion,
					testAccountID,
					tt.networkName,
					"",
					"",
					"",
					tt.memberName,
					"",
					nil,
				)
				require.NoError(t, err)
			}

			network, member, err := b.CreateNetwork(
				testRegion,
				testAccountID,
				tt.networkName,
				"",
				"",
				"",
				tt.memberName,
				"",
				nil,
			)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errSentinel)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, network.ID)
			assert.NotEmpty(t, network.Arn)
			assert.Equal(t, tt.networkName, network.Name)
			assert.Equal(t, "AVAILABLE", network.Status)
			assert.NotNil(t, network.CreationDate)
			assert.NotEmpty(t, member.ID)
			assert.Equal(t, tt.memberName, member.Name)
			assert.Equal(t, network.ID, member.NetworkID)
		})
	}
}

func TestInMemoryBackend_GetNetwork(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		networkID string
		wantErr   bool
	}{
		{
			name:    "get existing network",
			wantErr: false,
		},
		{
			name:      "not found returns error",
			networkID: "nonexistent",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			network, _, err := b.CreateNetwork(
				testRegion,
				testAccountID,
				"test-network",
				"",
				"",
				"",
				"member1",
				"",
				nil,
			)
			require.NoError(t, err)

			id := tt.networkID
			if id == "" {
				id = network.ID
			}

			got, err := b.GetNetwork(id)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, network.ID, got.ID)
			assert.Equal(t, "test-network", got.Name)
		})
	}
}

func TestInMemoryBackend_ListNetworks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantOrderedBy string
		networkNames  []string
		wantCount     int
	}{
		{
			name:         "empty list",
			networkNames: []string{},
			wantCount:    0,
		},
		{
			name:         "multiple networks sorted by name",
			networkNames: []string{"beta-net", "alpha-net", "gamma-net"},
			wantCount:    3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			for _, name := range tt.networkNames {
				_, _, err := b.CreateNetwork(testRegion, testAccountID, name, "", "", "", "m1", "", nil)
				require.NoError(t, err)
			}

			networks, err := b.ListNetworks()
			require.NoError(t, err)
			assert.Len(t, networks, tt.wantCount)

			if tt.wantCount > 1 {
				assert.Equal(t, "alpha-net", networks[0].Name)
			}
		})
	}
}

func TestInMemoryBackend_MemberLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		memberName string
	}{
		{
			name:       "full member lifecycle",
			memberName: "test-member",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			network, _, err := b.CreateNetwork(
				testRegion,
				testAccountID,
				"lifecycle-net",
				"",
				"",
				"",
				"initial",
				"",
				nil,
			)
			require.NoError(t, err)

			// CreateMember
			member, err := b.CreateMember(testRegion, testAccountID, network.ID, tt.memberName, "", nil)
			require.NoError(t, err)
			assert.NotEmpty(t, member.ID)
			assert.Equal(t, tt.memberName, member.Name)
			assert.Equal(t, "AVAILABLE", member.Status)

			// GetMember
			got, err := b.GetMember(network.ID, member.ID)
			require.NoError(t, err)
			assert.Equal(t, tt.memberName, got.Name)

			// ListMembers - should have initial + new member
			members, err := b.ListMembers(network.ID)
			require.NoError(t, err)
			assert.Len(t, members, 2)

			// DeleteMember
			err = b.DeleteMember(network.ID, member.ID)
			require.NoError(t, err)

			// Verify deletion
			_, err = b.GetMember(network.ID, member.ID)
			require.Error(t, err)
			assert.ErrorIs(t, err, awserr.ErrNotFound)
		})
	}
}

func TestInMemoryBackend_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "tag and untag resource"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			network, _, err := b.CreateNetwork(testRegion, testAccountID, "tagged-net", "", "", "", "m1", "", nil)
			require.NoError(t, err)

			// TagResource on network
			err = b.TagResource(network.Arn, map[string]string{"env": "test", "team": "backend"})
			require.NoError(t, err)

			// ListTagsForResource on network
			tags, err := b.ListTagsForResource(network.Arn)
			require.NoError(t, err)
			assert.Equal(t, "test", tags["env"])
			assert.Equal(t, "backend", tags["team"])

			// UntagResource on network
			err = b.UntagResource(network.Arn, []string{"team"})
			require.NoError(t, err)

			tags, err = b.ListTagsForResource(network.Arn)
			require.NoError(t, err)
			assert.Equal(t, "test", tags["env"])
			assert.NotContains(t, tags, "team")
		})
	}
}

func TestInMemoryBackend_TagOperationsOnMember(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "tag and untag member"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			network, initialMember, err := b.CreateNetwork(
				testRegion,
				testAccountID,
				"net1",
				"",
				"",
				"",
				"initial",
				"",
				nil,
			)
			require.NoError(t, err)

			// Tag the initial member
			err = b.TagResource(initialMember.Arn, map[string]string{"role": "primary"})
			require.NoError(t, err)

			// List tags on member
			tags, err := b.ListTagsForResource(initialMember.Arn)
			require.NoError(t, err)
			assert.Equal(t, "primary", tags["role"])

			// Untag member
			err = b.UntagResource(initialMember.Arn, []string{"role"})
			require.NoError(t, err)

			tags, err = b.ListTagsForResource(initialMember.Arn)
			require.NoError(t, err)
			assert.Empty(t, tags)

			// Verify ListTagsForResource on nonexistent ARN
			_, err = b.ListTagsForResource("arn:aws:managedblockchain:us-east-1:000000000000:networks/nonexistent")
			require.Error(t, err)
			assert.ErrorIs(t, err, awserr.ErrNotFound)

			_ = network
		})
	}
}

func TestInMemoryBackend_CreateMember_NetworkNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "member in nonexistent network"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			_, err := b.CreateMember(testRegion, testAccountID, "nonexistent-id", "m1", "", nil)
			require.Error(t, err)
			assert.ErrorIs(t, err, awserr.ErrNotFound)
		})
	}
}
