package resourcegroups_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

func TestResourceGroups_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *resourcegroups.InMemoryBackend)
		verify func(t *testing.T, b *resourcegroups.InMemoryBackend)
		name   string
	}{
		{
			name:  "empty_backend",
			setup: func(_ *testing.T, _ *resourcegroups.InMemoryBackend) {},
			verify: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				groups, _ := b.ListGroups(context.Background(), nil, "", 0)
				assert.Empty(t, groups)
			},
		},
		{
			name: "group_preserved",
			setup: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateGroup(context.Background(),
					"my-group",
					"test description",
					&resourcegroups.ResourceQuery{Type: "TAG_FILTERS_1_0", Query: "{}"},
					nil,
					nil,
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				groups, _ := b.ListGroups(context.Background(), nil, "", 0)
				require.Len(t, groups, 1)
				assert.Equal(t, "my-group", groups[0].Name)
				assert.Equal(t, "test description", groups[0].Description)

				// Name-based lookup.
				g, err := b.GetGroup(context.Background(), "my-group")
				require.NoError(t, err)
				assert.Equal(t, "my-group", g.Name)
			},
		},
		{
			name: "arn_index_rebuilt",
			setup: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateGroup(context.Background(), "indexed-group", "desc", nil, nil, nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				groups, _ := b.ListGroups(context.Background(), nil, "", 0)
				require.Len(t, groups, 1)

				// ARN-based tag lookup validates ARN index was rebuilt.
				tagMap, err := b.GetTagsByARN(context.Background(), groups[0].ARN)
				require.NoError(t, err)
				assert.NotNil(t, tagMap)
			},
		},
		{
			name: "tags_preserved",
			setup: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				g, err := b.CreateGroup(context.Background(), "tagged-group", "", nil, nil, nil)
				require.NoError(t, err)

				_, err = b.AddTagsByARN(context.Background(), g.ARN, map[string]string{"owner": "alice"})
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				groups, _ := b.ListGroups(context.Background(), nil, "", 0)
				require.Len(t, groups, 1)

				tagMap, err := b.GetTagsByARN(context.Background(), groups[0].ARN)
				require.NoError(t, err)
				assert.Equal(t, "alice", tagMap["owner"])
			},
		},
		{
			name: "tag_sync_task_preserved",
			setup: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateGroup(context.Background(), "sync-group", "", nil, nil, nil)
				require.NoError(t, err)

				_, err = b.StartTagSyncTask(
					context.Background(), "sync-group", "arn:aws:iam::123456789012:role/sync-role", "k", "v", nil,
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				assert.Equal(t, 1, resourcegroups.TagSyncTaskCount(b))

				tasks, _, err := b.ListTagSyncTasks(context.Background(), nil, "", 0)
				require.NoError(t, err)
				require.Len(t, tasks, 1)
				assert.Equal(t, "sync-group", tasks[0].GroupName)
				assert.Equal(t, "ACTIVE", tasks[0].Status)

				task, err := b.GetTagSyncTask(context.Background(), tasks[0].TaskArn)
				require.NoError(t, err)
				assert.Equal(t, tasks[0].TaskArn, task.TaskArn)
			},
		},
		{
			name: "group_configuration_preserved",
			setup: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateGroup(context.Background(), "cfg-group", "", nil, nil, nil)
				require.NoError(t, err)

				err = b.PutGroupConfiguration(
					context.Background(),
					"cfg-group",
					[]resourcegroups.GroupConfigurationItem{
						{Type: "AWS::ResourceGroups::Generic"},
					},
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				assert.Equal(t, 1, resourcegroups.GroupConfigurationCount(b))

				items, err := b.GetGroupConfigurationItems(context.Background(), "cfg-group")
				require.NoError(t, err)
				require.Len(t, items, 1)
				assert.Equal(t, "AWS::ResourceGroups::Generic", items[0].Type)
			},
		},
		{
			name: "group_resources_and_grouping_statuses_preserved",
			setup: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateGroup(context.Background(), "res-group", "", nil, nil, nil)
				require.NoError(t, err)

				_, err = b.GroupResources(
					context.Background(), "res-group", []string{"arn:aws:s3:::my-bucket"},
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				assert.Equal(t, 1, resourcegroups.GroupResourceCount(b))

				resources, _, err := b.ListGroupResources(context.Background(), "res-group", nil, "", 0)
				require.NoError(t, err)
				require.Len(t, resources, 1)
				assert.Equal(t, "arn:aws:s3:::my-bucket", resources[0].ResourceArn)

				statuses, _, err := b.ListGroupingStatuses(context.Background(), "res-group", "", 0)
				require.NoError(t, err)
				require.Len(t, statuses, 1)
				assert.Equal(t, "SUCCESS", statuses[0].Status)
			},
		},
		{
			name: "account_settings_preserved",
			setup: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				err := b.UpdateAccountSettings("ACTIVE")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				settings := b.GetAccountSettings()
				assert.Equal(t, "ACTIVE", settings.GroupLifecycleEventsDesiredStatus)
				assert.Equal(t, "ACTIVE", settings.GroupLifecycleEventsStatus)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := resourcegroups.NewInMemoryBackend("123456789012", "us-east-1")
			tt.setup(t, b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := resourcegroups.NewInMemoryBackend("123456789012", "us-east-1")
			err := b2.Restore(t.Context(), snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}

// Test_PersistenceVersionMismatch verifies that Restore discards an
// incompatible (or version-less) snapshot and starts empty rather than
// partially decoding it as the current shape.
func Test_PersistenceVersionMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{name: "explicit_future_version", data: []byte(`{"version":99}`)},
		{name: "absent_version_decodes_as_zero", data: []byte(`{}`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := resourcegroups.NewInMemoryBackend("123456789012", "us-east-1")

			_, err := b.CreateGroup(context.Background(), "pre-existing", "", nil, nil, nil)
			require.NoError(t, err)
			require.Equal(t, 1, resourcegroups.GroupCount(b))

			require.NoError(t, b.Restore(t.Context(), tt.data))

			assert.Equal(t, 0, resourcegroups.GroupCount(b))
			assert.Equal(t, 0, resourcegroups.TagSyncTaskCount(b))

			groups, _ := b.ListGroups(context.Background(), nil, "", 0)
			assert.Empty(t, groups)
		})
	}
}

// Test_PersistenceFullStateRoundTrip exercises every store.Table and every
// raw-left persisted map together in one Snapshot -> Restore cycle, including
// the derived groupsByARN index (rebuilt purely from the ARN embedded in each
// restored Group, never itself persisted).
func Test_PersistenceFullStateRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := resourcegroups.NewInMemoryBackend("123456789012", "us-east-1")

	g1, err := b.CreateGroup(ctx, "group-one", "first", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateGroup(ctx, "group-two", "second",
		&resourcegroups.ResourceQuery{Type: "TAG_FILTERS_1_0", Query: "{}"}, nil, nil)
	require.NoError(t, err)

	require.NoError(t, b.PutGroupConfiguration(ctx, "group-one", []resourcegroups.GroupConfigurationItem{
		{Type: "AWS::ResourceGroups::Generic"},
	}))

	_, err = b.GroupResources(ctx, "group-one", []string{"arn:aws:s3:::bucket-a"})
	require.NoError(t, err)

	_, err = b.StartTagSyncTask(ctx, "group-one", "arn:aws:iam::123456789012:role/sync", "k", "v", nil)
	require.NoError(t, err)

	require.NoError(t, b.UpdateAccountSettings("ACTIVE"))

	require.Equal(t, 2, resourcegroups.GroupCount(b))
	require.Equal(t, 1, resourcegroups.TagSyncTaskCount(b))
	require.Equal(t, 1, resourcegroups.GroupConfigurationCount(b))
	require.Equal(t, 1, resourcegroups.GroupResourceCount(b))

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(snap, &raw))
	assert.Contains(t, raw, "version")

	b2 := resourcegroups.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 2, resourcegroups.GroupCount(b2))
	assert.Equal(t, 1, resourcegroups.TagSyncTaskCount(b2))
	assert.Equal(t, 1, resourcegroups.GroupConfigurationCount(b2))
	assert.Equal(t, 1, resourcegroups.GroupResourceCount(b2))

	got1, err := b2.GetGroup(ctx, "group-one")
	require.NoError(t, err)
	assert.Equal(t, "first", got1.Description)
	assert.Equal(t, g1.ARN, got1.ARN)

	items, err := b2.GetGroupConfigurationItems(ctx, "group-one")
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "AWS::ResourceGroups::Generic", items[0].Type)

	resources, _, err := b2.ListGroupResources(ctx, "group-one", nil, "", 0)
	require.NoError(t, err)
	require.Len(t, resources, 1)
	assert.Equal(t, "arn:aws:s3:::bucket-a", resources[0].ResourceArn)

	statuses, _, err := b2.ListGroupingStatuses(ctx, "group-one", "", 0)
	require.NoError(t, err)
	require.Len(t, statuses, 1)
	assert.Equal(t, "SUCCESS", statuses[0].Status)

	tasks, _, err := b2.ListTagSyncTasks(ctx, nil, "", 0)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "group-one", tasks[0].GroupName)

	settings := b2.GetAccountSettings()
	assert.Equal(t, "ACTIVE", settings.GroupLifecycleEventsDesiredStatus)

	// ARN-based lookup must still work post-restore, proving the groupsByARN
	// index (never itself persisted) was correctly rebuilt from the restored
	// groups table.
	tagMap, err := b2.GetTagsByARN(ctx, got1.ARN)
	require.NoError(t, err)
	assert.NotNil(t, tagMap)
}
