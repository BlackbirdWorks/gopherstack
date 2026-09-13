package codestarconnections_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codestarconnectionssdk "github.com/aws/aws-sdk-go-v2/service/codestarconnections"
	codestarconnectionstypes "github.com/aws/aws-sdk-go-v2/service/codestarconnections/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codestarconnections"
)

// TestRealClient_HostRepositoryLinkAndSyncConfig drives every remaining uncovered op
// through a real aws-sdk-go-v2 codestarconnections client:
// CreateSyncConfiguration, DeleteHost, DeleteRepositoryLink, GetHost,
// GetRepositoryLink, GetRepositorySyncStatus, GetResourceSyncStatus,
// GetSyncBlockerSummary, GetSyncConfiguration, ListHosts,
// ListRepositoryLinks, ListRepositorySyncDefinitions,
// ListSyncConfigurations, TagResource, UntagResource, UpdateRepositoryLink,
// UpdateSyncBlocker, UpdateSyncConfiguration.
func TestRealClient_HostRepositoryLinkAndSyncConfig(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "host lifecycle: GetHost, ListHosts, DeleteHost", run: func(t *testing.T) {
			t.Helper()

			h := codestarconnections.NewHandler(
				codestarconnections.NewInMemoryBackend("123456789012", "us-east-1"),
			)
			client := newTestCodeStarConnectionsClient(t, h)
			ctx := t.Context()

			created, err := client.CreateHost(ctx, &codestarconnectionssdk.CreateHostInput{
				Name:             aws.String("slice25-host"),
				ProviderType:     codestarconnectionstypes.ProviderTypeGithubEnterpriseServer,
				ProviderEndpoint: aws.String("https://ghe.example.com"),
			})
			require.NoError(t, err)
			hostArn := aws.ToString(created.HostArn)
			require.NotEmpty(t, hostArn)

			got, err := client.GetHost(
				ctx,
				&codestarconnectionssdk.GetHostInput{HostArn: aws.String(hostArn)},
			)
			require.NoError(t, err)
			assert.Equal(t, "https://ghe.example.com", aws.ToString(got.ProviderEndpoint))

			listOut, err := client.ListHosts(ctx, &codestarconnectionssdk.ListHostsInput{})
			require.NoError(t, err)
			var found bool
			for _, host := range listOut.Hosts {
				if aws.ToString(host.HostArn) == hostArn {
					found = true
				}
			}
			assert.True(t, found, "created host must appear in ListHosts")

			_, err = client.DeleteHost(
				ctx,
				&codestarconnectionssdk.DeleteHostInput{HostArn: aws.String(hostArn)},
			)
			require.NoError(t, err)

			_, err = client.GetHost(
				ctx,
				&codestarconnectionssdk.GetHostInput{HostArn: aws.String(hostArn)},
			)
			require.Error(t, err)
		}},
		{name: "repository link and tags lifecycle", run: func(t *testing.T) {
			t.Helper()

			h := codestarconnections.NewHandler(
				codestarconnections.NewInMemoryBackend("123456789012", "us-east-1"),
			)
			client := newTestCodeStarConnectionsClient(t, h)
			ctx := t.Context()

			conn, err := client.CreateConnection(ctx, &codestarconnectionssdk.CreateConnectionInput{
				ConnectionName: aws.String("slice25-connection"),
				ProviderType:   codestarconnectionstypes.ProviderTypeGithub,
			})
			require.NoError(t, err)
			connArn := aws.ToString(conn.ConnectionArn)
			require.NotEmpty(t, connArn)

			created, err := client.CreateRepositoryLink(
				ctx,
				&codestarconnectionssdk.CreateRepositoryLinkInput{
					ConnectionArn:  aws.String(connArn),
					OwnerId:        aws.String("slice25-owner"),
					RepositoryName: aws.String("slice25-repo"),
				},
			)
			require.NoError(t, err)
			linkID := aws.ToString(created.RepositoryLinkInfo.RepositoryLinkId)
			require.NotEmpty(t, linkID)

			got, err := client.GetRepositoryLink(ctx, &codestarconnectionssdk.GetRepositoryLinkInput{
				RepositoryLinkId: aws.String(linkID),
			})
			require.NoError(t, err)
			assert.Equal(t, "slice25-repo", aws.ToString(got.RepositoryLinkInfo.RepositoryName))

			listOut, err := client.ListRepositoryLinks(
				ctx,
				&codestarconnectionssdk.ListRepositoryLinksInput{},
			)
			require.NoError(t, err)
			var found bool
			for _, link := range listOut.RepositoryLinks {
				if aws.ToString(link.RepositoryLinkId) == linkID {
					found = true
				}
			}
			assert.True(t, found, "created repository link must appear in ListRepositoryLinks")

			updated, err := client.UpdateRepositoryLink(
				ctx,
				&codestarconnectionssdk.UpdateRepositoryLinkInput{
					RepositoryLinkId: aws.String(linkID),
					ConnectionArn:    aws.String(connArn),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, connArn, aws.ToString(updated.RepositoryLinkInfo.ConnectionArn))

			linkArn := aws.ToString(got.RepositoryLinkInfo.RepositoryLinkArn)
			require.NotEmpty(t, linkArn)

			_, err = client.TagResource(ctx, &codestarconnectionssdk.TagResourceInput{
				ResourceArn: aws.String(linkArn),
				Tags: []codestarconnectionstypes.Tag{
					{Key: aws.String("env"), Value: aws.String("test")},
				},
			})
			require.NoError(t, err)

			tagsOut, err := client.ListTagsForResource(
				ctx,
				&codestarconnectionssdk.ListTagsForResourceInput{
					ResourceArn: aws.String(linkArn),
				},
			)
			require.NoError(t, err)
			require.Len(t, tagsOut.Tags, 1)
			assert.Equal(t, "env", aws.ToString(tagsOut.Tags[0].Key))

			_, err = client.UntagResource(ctx, &codestarconnectionssdk.UntagResourceInput{
				ResourceArn: aws.String(linkArn),
				TagKeys:     []string{"env"},
			})
			require.NoError(t, err)

			tagsOut, err = client.ListTagsForResource(
				ctx,
				&codestarconnectionssdk.ListTagsForResourceInput{
					ResourceArn: aws.String(linkArn),
				},
			)
			require.NoError(t, err)
			assert.Empty(t, tagsOut.Tags)

			_, err = client.DeleteRepositoryLink(ctx, &codestarconnectionssdk.DeleteRepositoryLinkInput{
				RepositoryLinkId: aws.String(linkID),
			})
			require.NoError(t, err)

			_, err = client.GetRepositoryLink(ctx, &codestarconnectionssdk.GetRepositoryLinkInput{
				RepositoryLinkId: aws.String(linkID),
			})
			require.Error(t, err)
		}},
		{name: "sync configuration and status family", run: func(t *testing.T) {
			t.Helper()

			h := codestarconnections.NewHandler(
				codestarconnections.NewInMemoryBackend("123456789012", "us-east-1"),
			)
			client := newTestCodeStarConnectionsClient(t, h)
			ctx := t.Context()

			conn, err := client.CreateConnection(ctx, &codestarconnectionssdk.CreateConnectionInput{
				ConnectionName: aws.String("slice25-sync-connection"),
				ProviderType:   codestarconnectionstypes.ProviderTypeGithub,
			})
			require.NoError(t, err)

			link, err := client.CreateRepositoryLink(
				ctx,
				&codestarconnectionssdk.CreateRepositoryLinkInput{
					ConnectionArn:  conn.ConnectionArn,
					OwnerId:        aws.String("slice25-sync-owner"),
					RepositoryName: aws.String("slice25-sync-repo"),
				},
			)
			require.NoError(t, err)
			linkID := aws.ToString(link.RepositoryLinkInfo.RepositoryLinkId)

			_, err = client.CreateSyncConfiguration(
				ctx,
				&codestarconnectionssdk.CreateSyncConfigurationInput{
					Branch:           aws.String("main"),
					ConfigFile:       aws.String("template.yaml"),
					RepositoryLinkId: aws.String(linkID),
					ResourceName:     aws.String("slice25-resource"),
					RoleArn:          aws.String("arn:aws:iam::123456789012:role/sync-role"),
					SyncType:         codestarconnectionstypes.SyncConfigurationTypeCfnStackSync,
				},
			)
			require.NoError(t, err)

			got, err := client.GetSyncConfiguration(
				ctx,
				&codestarconnectionssdk.GetSyncConfigurationInput{
					ResourceName: aws.String("slice25-resource"),
					SyncType:     codestarconnectionstypes.SyncConfigurationTypeCfnStackSync,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "main", aws.ToString(got.SyncConfiguration.Branch))

			listOut, err := client.ListSyncConfigurations(
				ctx,
				&codestarconnectionssdk.ListSyncConfigurationsInput{
					RepositoryLinkId: aws.String(linkID),
					SyncType:         codestarconnectionstypes.SyncConfigurationTypeCfnStackSync,
				},
			)
			require.NoError(t, err)
			require.Len(t, listOut.SyncConfigurations, 1)

			updated, err := client.UpdateSyncConfiguration(
				ctx,
				&codestarconnectionssdk.UpdateSyncConfigurationInput{
					ResourceName: aws.String("slice25-resource"),
					SyncType:     codestarconnectionstypes.SyncConfigurationTypeCfnStackSync,
					Branch:       aws.String("develop"),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "develop", aws.ToString(updated.SyncConfiguration.Branch))

			repoSyncStatus, err := client.GetRepositorySyncStatus(
				ctx,
				&codestarconnectionssdk.GetRepositorySyncStatusInput{
					RepositoryLinkId: aws.String(linkID),
					Branch:           aws.String("develop"),
					SyncType:         codestarconnectionstypes.SyncConfigurationTypeCfnStackSync,
				},
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				codestarconnectionstypes.RepositorySyncStatusSucceeded,
				repoSyncStatus.LatestSync.Status,
			)

			resourceSyncStatus, err := client.GetResourceSyncStatus(
				ctx,
				&codestarconnectionssdk.GetResourceSyncStatusInput{
					ResourceName: aws.String("slice25-resource"),
					SyncType:     codestarconnectionstypes.SyncConfigurationTypeCfnStackSync,
				},
			)
			require.NoError(t, err)
			require.NotNil(t, resourceSyncStatus.LatestSync)

			defs, err := client.ListRepositorySyncDefinitions(
				ctx,
				&codestarconnectionssdk.ListRepositorySyncDefinitionsInput{
					RepositoryLinkId: aws.String(linkID),
					SyncType:         codestarconnectionstypes.SyncConfigurationTypeCfnStackSync,
				},
			)
			require.NoError(t, err)
			require.Len(t, defs.RepositorySyncDefinitions, 1)
			assert.Equal(t, "slice25-resource", aws.ToString(defs.RepositorySyncDefinitions[0].Target))

			blocker, err := h.Backend.CreateSyncBlocker(
				ctx,
				"slice25-resource",
				string(codestarconnectionstypes.SyncConfigurationTypeCfnStackSync),
				"AUTOMATED",
				"conflicting change",
			)
			require.NoError(t, err)

			summary, err := client.GetSyncBlockerSummary(
				ctx,
				&codestarconnectionssdk.GetSyncBlockerSummaryInput{
					ResourceName: aws.String("slice25-resource"),
					SyncType:     codestarconnectionstypes.SyncConfigurationTypeCfnStackSync,
				},
			)
			require.NoError(t, err)
			require.Len(t, summary.SyncBlockerSummary.LatestBlockers, 1)
			assert.Equal(t, blocker.ID, aws.ToString(summary.SyncBlockerSummary.LatestBlockers[0].Id))

			resolved, err := client.UpdateSyncBlocker(
				ctx,
				&codestarconnectionssdk.UpdateSyncBlockerInput{
					Id:             aws.String(blocker.ID),
					ResolvedReason: aws.String("fixed"),
					ResourceName:   aws.String("slice25-resource"),
					SyncType:       codestarconnectionstypes.SyncConfigurationTypeCfnStackSync,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, codestarconnectionstypes.BlockerStatusResolved, resolved.SyncBlocker.Status)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
