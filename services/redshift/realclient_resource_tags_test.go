package redshift_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

func TestRealClient_ClassicResourceTags(t *testing.T) {
	t.Parallel()

	prefix := "arn:aws:redshift:" + rtTestRegion + ":000000000000:"
	envTag := []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}

	tests := []struct {
		create     func(t *testing.T, c *redshiftsdk.Client) []types.Tag
		describe   func(t *testing.T, c *redshiftsdk.Client, keys []string) int
		name       string
		arn        string
		resourceTy string
	}{
		{
			name:       "parameter_group",
			arn:        prefix + "parametergroup:pg1",
			resourceTy: "parametergroup",
			create: func(t *testing.T, c *redshiftsdk.Client) []types.Tag {
				t.Helper()
				out, err := c.CreateClusterParameterGroup(t.Context(), &redshiftsdk.CreateClusterParameterGroupInput{
					ParameterGroupName: aws.String("pg1"), ParameterGroupFamily: aws.String("redshift-1.0"),
					Description: aws.String("d"), Tags: envTag,
				})
				require.NoError(t, err)

				return out.ClusterParameterGroup.Tags
			},
			describe: func(t *testing.T, c *redshiftsdk.Client, keys []string) int {
				t.Helper()
				out, err := c.DescribeClusterParameterGroups(t.Context(),
					&redshiftsdk.DescribeClusterParameterGroupsInput{TagKeys: keys})
				require.NoError(t, err)

				return len(out.ParameterGroups)
			},
		},
		{
			name:       "snapshot",
			arn:        prefix + "snapshot:c1/snap1",
			resourceTy: "snapshot",
			create: func(t *testing.T, c *redshiftsdk.Client) []types.Tag {
				t.Helper()
				out, err := c.CreateClusterSnapshot(t.Context(), &redshiftsdk.CreateClusterSnapshotInput{
					SnapshotIdentifier: aws.String("snap1"), ClusterIdentifier: aws.String("c1"), Tags: envTag,
				})
				require.NoError(t, err)

				return out.Snapshot.Tags
			},
			describe: func(t *testing.T, c *redshiftsdk.Client, keys []string) int {
				t.Helper()
				out, err := c.DescribeClusterSnapshots(t.Context(),
					&redshiftsdk.DescribeClusterSnapshotsInput{TagKeys: keys})
				require.NoError(t, err)

				return len(out.Snapshots)
			},
		},
		{
			name:       "event_subscription",
			arn:        prefix + "eventsubscription:sub1",
			resourceTy: "eventsubscription",
			create: func(t *testing.T, c *redshiftsdk.Client) []types.Tag {
				t.Helper()
				out, err := c.CreateEventSubscription(t.Context(), &redshiftsdk.CreateEventSubscriptionInput{
					SubscriptionName: aws.String(
						"sub1",
					), SnsTopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:t"),
					Tags: envTag,
				})
				require.NoError(t, err)

				return out.EventSubscription.Tags
			},
			describe: func(t *testing.T, c *redshiftsdk.Client, keys []string) int {
				t.Helper()
				out, err := c.DescribeEventSubscriptions(t.Context(),
					&redshiftsdk.DescribeEventSubscriptionsInput{TagKeys: keys})
				require.NoError(t, err)

				return len(out.EventSubscriptionsList)
			},
		},
		{
			name:       "idc_application",
			arn:        prefix + "redshiftidcapplication/app1",
			resourceTy: "redshiftidcapplication",
			create: func(t *testing.T, c *redshiftsdk.Client) []types.Tag {
				t.Helper()
				out, err := c.CreateRedshiftIdcApplication(t.Context(), &redshiftsdk.CreateRedshiftIdcApplicationInput{
					RedshiftIdcApplicationName: aws.String("app1"),
					IdcInstanceArn:             aws.String("arn:aws:sso:::instance/ssoins-1"),
					IdcDisplayName:             aws.String("App"),
					IamRoleArn:                 aws.String("arn:aws:iam::000000000000:role/r"),
					Tags:                       envTag,
				})
				require.NoError(t, err)

				return out.RedshiftIdcApplication.Tags
			},
			describe: func(t *testing.T, c *redshiftsdk.Client, _ []string) int {
				t.Helper()
				out, err := c.DescribeRedshiftIdcApplications(
					t.Context(),
					&redshiftsdk.DescribeRedshiftIdcApplicationsInput{},
				)
				require.NoError(t, err)
				require.Len(t, out.RedshiftIdcApplications, 1)

				return len(out.RedshiftIdcApplications[0].Tags)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := redshift.NewInMemoryBackend("000000000000", rtTestRegion)
			client := newTestRedshiftClient(t, redshift.NewHandler(backend))
			_, err := backend.CreateCluster("c1", "dc2.large", "dev", "admin", nil, "", redshift.CreateClusterOptions{})
			require.NoError(t, err)

			created := tt.create(t, client)
			require.Len(t, created, 1)
			assert.Equal(t, "env", aws.ToString(created[0].Key))

			listed, err := client.DescribeTags(t.Context(), &redshiftsdk.DescribeTagsInput{
				ResourceType: aws.String(tt.resourceTy),
			})
			require.NoError(t, err)
			require.Len(t, listed.TaggedResources, 1)
			assert.Equal(t, tt.arn, aws.ToString(listed.TaggedResources[0].ResourceName))

			_, err = client.CreateTags(t.Context(), &redshiftsdk.CreateTagsInput{
				ResourceName: aws.String(tt.arn),
				Tags:         []types.Tag{{Key: aws.String("team"), Value: aws.String("x")}},
			})
			require.NoError(t, err)

			listed, err = client.DescribeTags(
				t.Context(),
				&redshiftsdk.DescribeTagsInput{ResourceName: aws.String(tt.arn)},
			)
			require.NoError(t, err)
			assert.Len(t, listed.TaggedResources, 2)

			if tt.name != "idc_application" {
				assert.Equal(t, 1, tt.describe(t, client, []string{"env"}))
				assert.Equal(t, 0, tt.describe(t, client, []string{"nope"}))
			}

			_, err = client.DeleteTags(t.Context(), &redshiftsdk.DeleteTagsInput{
				ResourceName: aws.String(tt.arn), TagKeys: []string{"env", "team"},
			})
			require.NoError(t, err)

			listed, err = client.DescribeTags(
				t.Context(),
				&redshiftsdk.DescribeTagsInput{ResourceName: aws.String(tt.arn)},
			)
			require.NoError(t, err)
			assert.Empty(t, listed.TaggedResources)
		})
	}
}
