package resourcegroups_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	resourcegroupssdk "github.com/aws/aws-sdk-go-v2/service/resourcegroups"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroups/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_AccountAndGroupConfig drives every remaining uncovered op
// through a real aws-sdk-go-v2 resourcegroups client: CancelTagSyncTask,
// GetAccountSettings, GetGroupConfiguration, GetGroupQuery, GetTagSyncTask,
// ListGroupResources, ListGroups, ListTagSyncTasks, PutGroupConfiguration,
// SearchResources, StartTagSyncTask, Tag, Untag, UpdateAccountSettings,
// UpdateGroup, UpdateGroupQuery.
func TestRealClient_AccountAndGroupConfig(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "account settings get and update",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestResourceGroupsHandler(t)
				client := newTestResourceGroupsClient(t, h)
				ctx := t.Context()

				got, err := client.GetAccountSettings(ctx, &resourcegroupssdk.GetAccountSettingsInput{})
				require.NoError(t, err)
				require.NotNil(t, got.AccountSettings)

				updated, err := client.UpdateAccountSettings(
					ctx,
					&resourcegroupssdk.UpdateAccountSettingsInput{
						GroupLifecycleEventsDesiredStatus: types.GroupLifecycleEventsDesiredStatusActive,
					},
				)
				require.NoError(t, err)
				require.NotNil(t, updated.AccountSettings)
				assert.Equal(
					t,
					types.GroupLifecycleEventsDesiredStatusActive,
					updated.AccountSettings.GroupLifecycleEventsDesiredStatus,
				)
			},
		},
		{
			name: "group query and update: ListGroups, GetGroupQuery, UpdateGroup, UpdateGroupQuery",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestResourceGroupsHandler(t)
				client := newTestResourceGroupsClient(t, h)
				ctx := t.Context()

				name := "slice25-group"
				_, err := client.CreateGroup(ctx, &resourcegroupssdk.CreateGroupInput{
					Name: aws.String(name),
					ResourceQuery: &types.ResourceQuery{
						Type: types.QueryTypeTagFilters10,
						Query: aws.String(
							`{"ResourceTypeFilters":["AWS::AllSupported"],"TagFilters":[]}`,
						),
					},
				})
				require.NoError(t, err)

				listOut, err := client.ListGroups(ctx, &resourcegroupssdk.ListGroupsInput{})
				require.NoError(t, err)
				var found bool
				for _, g := range listOut.GroupIdentifiers {
					if aws.ToString(g.GroupName) == name {
						found = true
					}
				}
				assert.True(t, found, "created group must appear in ListGroups")

				gq, err := client.GetGroupQuery(
					ctx,
					&resourcegroupssdk.GetGroupQueryInput{Group: aws.String(name)},
				)
				require.NoError(t, err)
				require.NotNil(t, gq.GroupQuery)
				require.NotNil(t, gq.GroupQuery.ResourceQuery)
				assert.Equal(t, types.QueryTypeTagFilters10, gq.GroupQuery.ResourceQuery.Type)

				updatedGroup, err := client.UpdateGroup(ctx, &resourcegroupssdk.UpdateGroupInput{
					Group:       aws.String(name),
					Description: aws.String("updated description"),
				})
				require.NoError(t, err)
				require.NotNil(t, updatedGroup.Group)
				assert.Equal(t, "updated description", aws.ToString(updatedGroup.Group.Description))

				newQuery := &types.ResourceQuery{
					Type:  types.QueryTypeTagFilters10,
					Query: aws.String(`{"ResourceTypeFilters":["AWS::EC2::Instance"],"TagFilters":[]}`),
				}
				uq, err := client.UpdateGroupQuery(ctx, &resourcegroupssdk.UpdateGroupQueryInput{
					Group:         aws.String(name),
					ResourceQuery: newQuery,
				})
				require.NoError(t, err)
				require.NotNil(t, uq.GroupQuery)
				require.NotNil(t, uq.GroupQuery.ResourceQuery)
				assert.Equal(
					t,
					aws.ToString(newQuery.Query),
					aws.ToString(uq.GroupQuery.ResourceQuery.Query),
				)
			},
		},
		{
			name: "group configuration: PutGroupConfiguration, GetGroupConfiguration",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestResourceGroupsHandler(t)
				client := newTestResourceGroupsClient(t, h)
				ctx := t.Context()

				name := "slice25-config-group"
				_, err := client.CreateGroup(ctx, &resourcegroupssdk.CreateGroupInput{
					Name: aws.String(name),
					Configuration: []types.GroupConfigurationItem{
						{Type: aws.String("AWS::EC2::CapacityReservationPool")},
					},
				})
				require.NoError(t, err)

				_, err = client.PutGroupConfiguration(ctx, &resourcegroupssdk.PutGroupConfigurationInput{
					Group: aws.String(name),
					Configuration: []types.GroupConfigurationItem{
						{Type: aws.String("AWS::EC2::CapacityReservationPool")},
					},
				})
				require.NoError(t, err)

				got, err := client.GetGroupConfiguration(
					ctx,
					&resourcegroupssdk.GetGroupConfigurationInput{Group: aws.String(name)},
				)
				require.NoError(t, err)
				require.NotNil(t, got.GroupConfiguration)
				require.Len(t, got.GroupConfiguration.Configuration, 1)
				assert.Equal(
					t,
					"AWS::EC2::CapacityReservationPool",
					aws.ToString(got.GroupConfiguration.Configuration[0].Type),
				)
				assert.Equal(t, types.GroupConfigurationStatusUpdateComplete, got.GroupConfiguration.Status)
			},
		},
		{
			name: "resources: GroupResources, ListGroupResources, SearchResources",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestResourceGroupsHandler(t)
				client := newTestResourceGroupsClient(t, h)
				ctx := t.Context()

				name := "slice25-resources-group"
				_, err := client.CreateGroup(
					ctx,
					&resourcegroupssdk.CreateGroupInput{Name: aws.String(name)},
				)
				require.NoError(t, err)

				arn := "arn:aws:ec2:us-east-1:000000000000:instance/i-0123456789abcdef0"
				grouped, err := client.GroupResources(ctx, &resourcegroupssdk.GroupResourcesInput{
					Group:        aws.String(name),
					ResourceArns: []string{arn},
				})
				require.NoError(t, err)
				assert.Contains(t, grouped.Succeeded, arn)

				listOut, err := client.ListGroupResources(
					ctx,
					&resourcegroupssdk.ListGroupResourcesInput{Group: aws.String(name)},
				)
				require.NoError(t, err)
				require.Len(t, listOut.Resources, 1)
				assert.Equal(t, arn, aws.ToString(listOut.Resources[0].Identifier.ResourceArn))

				searchOut, err := client.SearchResources(ctx, &resourcegroupssdk.SearchResourcesInput{
					ResourceQuery: &types.ResourceQuery{
						Type:  types.QueryTypeTagFilters10,
						Query: aws.String(`{"ResourceTypeFilters":["AWS::AllSupported"],"TagFilters":[]}`),
					},
				})
				require.NoError(t, err)
				assert.NotNil(t, searchOut.ResourceIdentifiers)
			},
		},
		{
			name: "tag sync task lifecycle: Start, Get, List, Cancel",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestResourceGroupsHandler(t)
				client := newTestResourceGroupsClient(t, h)
				ctx := t.Context()

				name := "slice25-tagsync-group"
				_, err := client.CreateGroup(
					ctx,
					&resourcegroupssdk.CreateGroupInput{Name: aws.String(name)},
				)
				require.NoError(t, err)

				started, err := client.StartTagSyncTask(ctx, &resourcegroupssdk.StartTagSyncTaskInput{
					Group:    aws.String(name),
					RoleArn:  aws.String("arn:aws:iam::000000000000:role/TagSyncRole"),
					TagKey:   aws.String("env"),
					TagValue: aws.String("prod"),
				})
				require.NoError(t, err)
				taskArn := aws.ToString(started.TaskArn)
				require.NotEmpty(t, taskArn)

				got, err := client.GetTagSyncTask(
					ctx,
					&resourcegroupssdk.GetTagSyncTaskInput{TaskArn: aws.String(taskArn)},
				)
				require.NoError(t, err)
				assert.Equal(t, "env", aws.ToString(got.TagKey))
				assert.Equal(t, "prod", aws.ToString(got.TagValue))

				listOut, err := client.ListTagSyncTasks(ctx, &resourcegroupssdk.ListTagSyncTasksInput{})
				require.NoError(t, err)
				var found bool
				for _, task := range listOut.TagSyncTasks {
					if aws.ToString(task.TaskArn) == taskArn {
						found = true
					}
				}
				assert.True(t, found, "started task must appear in ListTagSyncTasks")

				_, err = client.CancelTagSyncTask(
					ctx,
					&resourcegroupssdk.CancelTagSyncTaskInput{TaskArn: aws.String(taskArn)},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "Tag and Untag",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestResourceGroupsHandler(t)
				client := newTestResourceGroupsClient(t, h)
				ctx := t.Context()

				created, err := client.CreateGroup(
					ctx,
					&resourcegroupssdk.CreateGroupInput{Name: aws.String("slice25-tag-group")},
				)
				require.NoError(t, err)
				groupArn := aws.ToString(created.Group.GroupArn)
				require.NotEmpty(t, groupArn)

				tagged, err := client.Tag(ctx, &resourcegroupssdk.TagInput{
					Arn:  aws.String(groupArn),
					Tags: map[string]string{"team": "platform"},
				})
				require.NoError(t, err)
				assert.Equal(t, "platform", tagged.Tags["team"])

				untagged, err := client.Untag(ctx, &resourcegroupssdk.UntagInput{
					Arn:  aws.String(groupArn),
					Keys: []string{"team"},
				})
				require.NoError(t, err)
				assert.Contains(t, untagged.Keys, "team")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
