package resourcegroups_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

func TestResourceGroupsCreateGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		setup       func(b *resourcegroups.InMemoryBackend)
		tags        *tags.Tags
		name        string
		groupName   string
		description string
	}{
		{
			name:        "success",
			groupName:   "my-group",
			description: "test description",
		},
		{
			name:      "already_exists",
			groupName: "my-group",
			setup: func(b *resourcegroups.InMemoryBackend) {
				_, _ = b.CreateGroup(context.Background(), "my-group", "", nil, nil, nil)
			},
			wantErr: resourcegroups.ErrAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			g, err := b.CreateGroup(context.Background(), tt.groupName, tt.description, nil, tt.tags, nil)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.groupName, g.Name)
			assert.Contains(t, g.ARN, "arn:aws:resource-groups:")
			assert.Equal(t, tt.description, g.Description)
		})
	}
}

// TestCreateGroupAtomicConfig verifies that if configuration storage fails
// validation, the group is also not created (atomic).
func TestCreateGroupAtomicConfig(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateGroup(context.Background(), "atomic-group", "", nil, nil, []resourcegroups.GroupConfigurationItem{
		{Type: "AWS::Invalid::Type"},
	})
	require.Error(t, err)

	// Group must not have been created.
	_, err = b.GetGroup(context.Background(), "atomic-group")
	assert.ErrorIs(t, err, resourcegroups.ErrNotFound)
}

// TestCreateGroupMutualExclusivityBackend verifies the backend-level
// rejection of a group specifying both ResourceQuery and Configuration.
func TestCreateGroupMutualExclusivityBackend(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateGroup(context.Background(),
		"bad-group",
		"",
		&resourcegroups.ResourceQuery{Type: "TAG_FILTERS_1_0", Query: `{}`},
		nil,
		[]resourcegroups.GroupConfigurationItem{{Type: "AWS::EC2::CapacityReservationPool"}},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, resourcegroups.ErrValidation)
	assert.Contains(t, err.Error(), "cannot have both")

	// Group must not exist after the failed call.
	_, err = b.GetGroup(context.Background(), "bad-group")
	assert.ErrorIs(t, err, resourcegroups.ErrNotFound)
}

func TestResourceGroupsDeleteGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(b *resourcegroups.InMemoryBackend)
		name      string
		groupName string
	}{
		{
			name:      "success",
			groupName: "my-group",
			setup: func(b *resourcegroups.InMemoryBackend) {
				_, _ = b.CreateGroup(context.Background(), "my-group", "", nil, nil, nil)
			},
		},
		{
			name:      "not_found",
			groupName: "nonexistent",
			wantErr:   resourcegroups.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			deleted, err := b.DeleteGroup(context.Background(), tt.groupName)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, deleted)

				return
			}
			require.NoError(t, err)
			require.NotNil(t, deleted)
			assert.Equal(t, tt.groupName, deleted.Name, "DeleteGroup must echo back the deleted group")
			groups, _ := b.ListGroups(context.Background(), nil, "", 0)
			assert.Empty(t, groups)
		})
	}
}

// TestDeleteGroup_ByARN verifies cascaded deletion when addressing by ARN.
func TestDeleteGroup_ByARN(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	g, err := b.CreateGroup(context.Background(), "del-by-arn", "", nil, nil, nil)
	require.NoError(t, err)

	deleted, err := b.DeleteGroup(context.Background(), g.ARN)
	require.NoError(t, err)
	assert.Equal(t, "del-by-arn", deleted.Name)

	_, err = b.GetGroup(context.Background(), "del-by-arn")
	assert.ErrorIs(t, err, resourcegroups.ErrNotFound)
}

func TestResourceGroupsGetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(b *resourcegroups.InMemoryBackend)
		groupName string
		wantName  string // expected g.Name; defaults to groupName when empty
		wantErr   error
		wantTag   string
	}{
		{
			name:      "success",
			groupName: "my-group",
			setup: func(b *resourcegroups.InMemoryBackend) {
				tgs := tags.FromMap("test.rg", map[string]string{"env": "test"})
				_, _ = b.CreateGroup(context.Background(), "my-group", "desc", nil, tgs, nil)
			},
			wantTag: "test",
		},
		{
			name:      "not_found",
			groupName: "nonexistent",
			wantErr:   resourcegroups.ErrNotFound,
		},
		{
			name:      "arn_lookup",
			groupName: "arn:aws:resource-groups:us-east-1:000000000000:group/my-group",
			wantName:  "my-group",
			setup: func(b *resourcegroups.InMemoryBackend) {
				_, _ = b.CreateGroup(context.Background(), "my-group", "desc", nil, nil, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			g, err := b.GetGroup(context.Background(), tt.groupName)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			wantName := tt.groupName
			if tt.wantName != "" {
				wantName = tt.wantName
			}
			assert.Equal(t, wantName, g.Name)
			if tt.wantTag != "" {
				v, _ := g.Tags.Get("env")
				assert.Equal(t, tt.wantTag, v)
			}
		})
	}
}

// TestGetGroup_ByARN verifies that a group can be retrieved by ARN.
func TestGetGroup_ByARN(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	g, err := b.CreateGroup(context.Background(), "arn-group", "desc", nil, nil, nil)
	require.NoError(t, err)

	// Retrieve by ARN instead of name.
	got, err := b.GetGroup(context.Background(), g.ARN)
	require.NoError(t, err)
	assert.Equal(t, "arn-group", got.Name)
	assert.Equal(t, g.ARN, got.ARN)
}

func TestResourceGroupsListGroups(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateGroup(context.Background(), "group-a", "", nil, nil, nil)
	_, _ = b.CreateGroup(context.Background(), "group-b", "", nil, nil, nil)

	groups, _ := b.ListGroups(context.Background(), nil, "", 0)
	assert.Len(t, groups, 2)
}

// TestListGroups_Pagination verifies NextToken/MaxResults pagination.
func TestListGroups_Pagination(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	// Create 5 groups: a-group, b-group, c-group, d-group, e-group.
	for _, name := range []string{"e-group", "c-group", "a-group", "b-group", "d-group"} {
		_, err := b.CreateGroup(context.Background(), name, "", nil, nil, nil)
		require.NoError(t, err)
	}

	tests := []struct { //nolint:govet // field order optimized for readability
		name       string
		maxResults int
		wantNames  []string
		wantMore   bool
	}{
		{
			name:       "page_size_2",
			maxResults: 2,
			wantNames:  []string{"a-group", "b-group"},
			wantMore:   true,
		},
		{
			name:       "page_size_3",
			maxResults: 3,
			wantNames:  []string{"a-group", "b-group", "c-group"},
			wantMore:   true,
		},
		{
			name:       "page_size_5_all",
			maxResults: 5,
			wantNames:  []string{"a-group", "b-group", "c-group", "d-group", "e-group"},
			wantMore:   false,
		},
		{
			name:       "page_size_0_returns_all",
			maxResults: 0,
			wantNames:  []string{"a-group", "b-group", "c-group", "d-group", "e-group"},
			wantMore:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			page, token := b.ListGroups(context.Background(), nil, "", tt.maxResults)
			names := make([]string, len(page))
			for i, g := range page {
				names[i] = g.Name
			}
			assert.Equal(t, tt.wantNames, names)
			if tt.wantMore {
				assert.NotEmpty(t, token)
			} else {
				assert.Empty(t, token)
			}
		})
	}
}

// TestListGroups_PaginationResume verifies sequential token-based listing.
func TestListGroups_PaginationResume(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	for _, name := range []string{"a-group", "b-group", "c-group", "d-group", "e-group"} {
		_, err := b.CreateGroup(context.Background(), name, "", nil, nil, nil)
		require.NoError(t, err)
	}

	// Collect all names across pages of 2.
	allNames := make([]string, 0, 5)

	page, token := b.ListGroups(context.Background(), nil, "", 2)
	for _, g := range page {
		allNames = append(allNames, g.Name)
	}
	require.NotEmpty(t, token)

	page, token = b.ListGroups(context.Background(), nil, token, 2)
	for _, g := range page {
		allNames = append(allNames, g.Name)
	}
	require.NotEmpty(t, token)

	page, token = b.ListGroups(context.Background(), nil, token, 2)
	for _, g := range page {
		allNames = append(allNames, g.Name)
	}
	assert.Empty(t, token)

	assert.Equal(t, []string{"a-group", "b-group", "c-group", "d-group", "e-group"}, allNames)
}

// TestListGroupsNamePrefixFilter verifies the name-prefix filter.
func TestListGroupsNamePrefixFilter(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	for _, name := range []string{"app-prod", "app-staging", "data-prod", "infra-shared"} {
		_, err := b.CreateGroup(context.Background(), name, "", nil, nil, nil)
		require.NoError(t, err)
	}

	tests := []struct {
		name      string
		prefix    string
		wantNames []string
	}{
		{
			name:      "prefix_app",
			prefix:    "app",
			wantNames: []string{"app-prod", "app-staging"},
		},
		{
			name:      "prefix_data",
			prefix:    "data",
			wantNames: []string{"data-prod"},
		},
		{
			name:      "prefix_infra",
			prefix:    "infra",
			wantNames: []string{"infra-shared"},
		},
		{
			name:      "prefix_no_match",
			prefix:    "xyz",
			wantNames: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			filters := []resourcegroups.ListGroupsFilter{
				{Name: "name-prefix", Values: []string{tt.prefix}},
			}

			groups, _ := b.ListGroups(context.Background(), filters, "", 0)
			names := make([]string, len(groups))
			for i, g := range groups {
				names[i] = g.Name
			}
			assert.Equal(t, tt.wantNames, names)
		})
	}
}

// TestListGroups_FilterAndPagination verifies config-type filter with pagination.
func TestListGroups_FilterAndPagination(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	for i := range 4 {
		name := fmt.Sprintf("cap-pool-%d", i)
		_, err := b.CreateGroup(context.Background(), name, "", nil, nil, nil)
		require.NoError(t, err)
		err = b.PutGroupConfiguration(context.Background(), name, []resourcegroups.GroupConfigurationItem{
			{Type: "AWS::EC2::CapacityReservationPool"},
		})
		require.NoError(t, err)
	}

	// Also create groups with a different config type.
	for i := range 3 {
		name := fmt.Sprintf("host-mgmt-%d", i)
		_, err := b.CreateGroup(context.Background(), name, "", nil, nil, nil)
		require.NoError(t, err)
		err = b.PutGroupConfiguration(context.Background(), name, []resourcegroups.GroupConfigurationItem{
			{Type: "AWS::EC2::HostManagement"},
		})
		require.NoError(t, err)
	}

	filter := []resourcegroups.ListGroupsFilter{
		{Name: "configuration-type", Values: []string{"AWS::EC2::CapacityReservationPool"}},
	}

	// Page 1 of 2 from the filtered set.
	page1, tok1 := b.ListGroups(context.Background(), filter, "", 2)
	assert.Len(t, page1, 2)
	require.NotEmpty(t, tok1)

	for _, g := range page1 {
		assert.True(t, strings.HasPrefix(g.Name, "cap-pool-"))
	}

	// Page 2.
	page2, tok2 := b.ListGroups(context.Background(), filter, tok1, 2)
	assert.Len(t, page2, 2)
	assert.Empty(t, tok2)

	for _, g := range page2 {
		assert.True(t, strings.HasPrefix(g.Name, "cap-pool-"))
	}
}

// TestUpdateGroup_FieldPersistence verifies each field updates independently.
func TestUpdateGroup_FieldPersistence(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup(context.Background(), "update-me", "initial desc", nil, nil, nil)
	require.NoError(t, err)

	// Set criticality.
	g, err := b.UpdateGroup(context.Background(), "update-me", "initial desc", "", 3)
	require.NoError(t, err)
	assert.Equal(t, 3, g.Criticality)
	assert.Equal(t, "initial desc", g.Description)
	assert.Empty(t, g.DisplayName)

	// Set display name (criticality=0 means no change).
	g, err = b.UpdateGroup(context.Background(), "update-me", "initial desc", "My Display Name", 0)
	require.NoError(t, err)
	assert.Equal(t, 3, g.Criticality) // preserved
	assert.Equal(t, "My Display Name", g.DisplayName)

	// Change description.
	g, err = b.UpdateGroup(context.Background(), "update-me", "new desc", "", 0)
	require.NoError(t, err)
	assert.Equal(t, "new desc", g.Description)
	assert.Equal(t, 3, g.Criticality)                 // still preserved
	assert.Equal(t, "My Display Name", g.DisplayName) // still preserved
}

// TestPutGroupConfiguration_DeepCopy verifies parameter values are properly
// deep-copied and mutations in the caller do not affect stored state.
func TestPutGroupConfiguration_DeepCopy(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateGroup(context.Background(), "g1", "", nil, nil, nil)

	params := []resourcegroups.GroupConfigurationParameter{
		{Name: "allowed-resource-types", Values: []string{"v1", "v2"}},
	}
	items := []resourcegroups.GroupConfigurationItem{
		{Type: "AWS::ResourceGroups::Generic", Parameters: params},
	}

	require.NoError(t, b.PutGroupConfiguration(context.Background(), "g1", items))

	// Mutate the original slice after storing.
	params[0].Values[0] = "mutated"

	got, err := b.GetGroupConfigurationItems(context.Background(), "g1")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Len(t, got[0].Parameters, 1)
	// Stored value should not be affected by the mutation.
	assert.Equal(t, "v1", got[0].Parameters[0].Values[0])
}

// TestCloneConfigItems_NilInput verifies nil config input returns empty slice.
func TestCloneConfigItems_NilInput(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateGroup(context.Background(), "g1", "", nil, nil, nil)

	items, err := b.GetGroupConfigurationItems(context.Background(), "g1")
	require.NoError(t, err)
	assert.NotNil(t, items)
	assert.Empty(t, items)
}

// TestAddGroupInternal verifies the seed helper works for tests.
func TestAddGroupInternal(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	g := resourcegroups.AddGroupInternal(b, "seeded-group", "seeded desc")

	assert.Equal(t, "seeded-group", g.Name)
	assert.Equal(t, 1, resourcegroups.GroupCount(b))
}
