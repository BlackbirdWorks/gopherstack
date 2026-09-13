package codeconnections_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codeconnectionssdk "github.com/aws/aws-sdk-go-v2/service/codeconnections"
	codeconnectionstypes "github.com/aws/aws-sdk-go-v2/service/codeconnections/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

// TestCodeConnections_TypedSlice25 drives every remaining uncovered op
// through a real aws-sdk-go-v2 codeconnections client: CreateRepositoryLink,
// CreateSyncConfiguration, DeleteHost, DeleteRepositoryLink,
// GetRepositoryLink, GetRepositorySyncStatus, GetResourceSyncStatus,
// GetSyncBlockerSummary, GetSyncConfiguration, ListRepositoryLinks,
// ListRepositorySyncDefinitions, ListSyncConfigurations,
// ListTagsForResource, TagResource, UntagResource, UpdateRepositoryLink,
// UpdateSyncBlocker, UpdateSyncConfiguration.
func TestCodeConnections_TypedSlice25(t *testing.T) {
	t.Parallel()

	t.Run("host lifecycle: DeleteHost", func(t *testing.T) {
		t.Parallel()

		h := codeconnections.NewHandler(
			codeconnections.NewInMemoryBackend("123456789012", "us-east-1"),
		)
		client := newTestCodeConnectionsClient(t, h)
		ctx := t.Context()

		created, err := client.CreateHost(ctx, &codeconnectionssdk.CreateHostInput{
			Name:             aws.String("slice25-host"),
			ProviderType:     codeconnectionstypes.ProviderTypeGithubEnterpriseServer,
			ProviderEndpoint: aws.String("https://ghe.example.com"),
		})
		require.NoError(t, err)
		hostArn := aws.ToString(created.HostArn)
		require.NotEmpty(t, hostArn)

		_, err = client.DeleteHost(
			ctx,
			&codeconnectionssdk.DeleteHostInput{HostArn: aws.String(hostArn)},
		)
		require.NoError(t, err)

		_, err = client.GetHost(ctx, &codeconnectionssdk.GetHostInput{HostArn: aws.String(hostArn)})
		require.Error(t, err)
	})

	t.Run("repository link and tags lifecycle", func(t *testing.T) {
		t.Parallel()

		h := codeconnections.NewHandler(
			codeconnections.NewInMemoryBackend("123456789012", "us-east-1"),
		)
		client := newTestCodeConnectionsClient(t, h)
		ctx := t.Context()

		conn, err := client.CreateConnection(ctx, &codeconnectionssdk.CreateConnectionInput{
			ConnectionName: aws.String("slice25-connection"),
			ProviderType:   codeconnectionstypes.ProviderTypeGithub,
		})
		require.NoError(t, err)
		connArn := aws.ToString(conn.ConnectionArn)
		require.NotEmpty(t, connArn)

		created, err := client.CreateRepositoryLink(
			ctx,
			&codeconnectionssdk.CreateRepositoryLinkInput{
				ConnectionArn:  aws.String(connArn),
				OwnerId:        aws.String("slice25-owner"),
				RepositoryName: aws.String("slice25-repo"),
			},
		)
		require.NoError(t, err)
		linkID := aws.ToString(created.RepositoryLinkInfo.RepositoryLinkId)
		require.NotEmpty(t, linkID)

		got, err := client.GetRepositoryLink(ctx, &codeconnectionssdk.GetRepositoryLinkInput{
			RepositoryLinkId: aws.String(linkID),
		})
		require.NoError(t, err)
		assert.Equal(t, "slice25-repo", aws.ToString(got.RepositoryLinkInfo.RepositoryName))

		listOut, err := client.ListRepositoryLinks(
			ctx,
			&codeconnectionssdk.ListRepositoryLinksInput{},
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
			&codeconnectionssdk.UpdateRepositoryLinkInput{
				RepositoryLinkId: aws.String(linkID),
				ConnectionArn:    aws.String(connArn),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, connArn, aws.ToString(updated.RepositoryLinkInfo.ConnectionArn))

		linkArn := aws.ToString(got.RepositoryLinkInfo.RepositoryLinkArn)
		require.NotEmpty(t, linkArn)

		_, err = client.TagResource(ctx, &codeconnectionssdk.TagResourceInput{
			ResourceArn: aws.String(linkArn),
			Tags: []codeconnectionstypes.Tag{
				{Key: aws.String("env"), Value: aws.String("test")},
			},
		})
		require.NoError(t, err)

		tagsOut, err := client.ListTagsForResource(
			ctx,
			&codeconnectionssdk.ListTagsForResourceInput{
				ResourceArn: aws.String(linkArn),
			},
		)
		require.NoError(t, err)
		require.Len(t, tagsOut.Tags, 1)
		assert.Equal(t, "env", aws.ToString(tagsOut.Tags[0].Key))

		_, err = client.UntagResource(ctx, &codeconnectionssdk.UntagResourceInput{
			ResourceArn: aws.String(linkArn),
			TagKeys:     []string{"env"},
		})
		require.NoError(t, err)

		tagsOut, err = client.ListTagsForResource(ctx, &codeconnectionssdk.ListTagsForResourceInput{
			ResourceArn: aws.String(linkArn),
		})
		require.NoError(t, err)
		assert.Empty(t, tagsOut.Tags)

		_, err = client.DeleteRepositoryLink(ctx, &codeconnectionssdk.DeleteRepositoryLinkInput{
			RepositoryLinkId: aws.String(linkID),
		})
		require.NoError(t, err)

		_, err = client.GetRepositoryLink(ctx, &codeconnectionssdk.GetRepositoryLinkInput{
			RepositoryLinkId: aws.String(linkID),
		})
		require.Error(t, err)
	})

	t.Run("sync configuration and status family", func(t *testing.T) {
		t.Parallel()

		h := codeconnections.NewHandler(
			codeconnections.NewInMemoryBackend("123456789012", "us-east-1"),
		)
		client := newTestCodeConnectionsClient(t, h)
		ctx := t.Context()

		conn, err := client.CreateConnection(ctx, &codeconnectionssdk.CreateConnectionInput{
			ConnectionName: aws.String("slice25-sync-connection"),
			ProviderType:   codeconnectionstypes.ProviderTypeGithub,
		})
		require.NoError(t, err)

		link, err := client.CreateRepositoryLink(ctx, &codeconnectionssdk.CreateRepositoryLinkInput{
			ConnectionArn:  conn.ConnectionArn,
			OwnerId:        aws.String("slice25-sync-owner"),
			RepositoryName: aws.String("slice25-sync-repo"),
		})
		require.NoError(t, err)
		linkID := aws.ToString(link.RepositoryLinkInfo.RepositoryLinkId)

		_, err = client.CreateSyncConfiguration(
			ctx,
			&codeconnectionssdk.CreateSyncConfigurationInput{
				Branch:           aws.String("main"),
				ConfigFile:       aws.String("template.yaml"),
				RepositoryLinkId: aws.String(linkID),
				ResourceName:     aws.String("slice25-resource"),
				RoleArn:          aws.String("arn:aws:iam::123456789012:role/sync-role"),
				SyncType:         codeconnectionstypes.SyncConfigurationTypeCfnStackSync,
			},
		)
		require.NoError(t, err)

		got, err := client.GetSyncConfiguration(ctx, &codeconnectionssdk.GetSyncConfigurationInput{
			ResourceName: aws.String("slice25-resource"),
			SyncType:     codeconnectionstypes.SyncConfigurationTypeCfnStackSync,
		})
		require.NoError(t, err)
		assert.Equal(t, "main", aws.ToString(got.SyncConfiguration.Branch))

		listOut, err := client.ListSyncConfigurations(
			ctx,
			&codeconnectionssdk.ListSyncConfigurationsInput{
				RepositoryLinkId: aws.String(linkID),
				SyncType:         codeconnectionstypes.SyncConfigurationTypeCfnStackSync,
			},
		)
		require.NoError(t, err)
		require.Len(t, listOut.SyncConfigurations, 1)

		updated, err := client.UpdateSyncConfiguration(
			ctx,
			&codeconnectionssdk.UpdateSyncConfigurationInput{
				ResourceName: aws.String("slice25-resource"),
				SyncType:     codeconnectionstypes.SyncConfigurationTypeCfnStackSync,
				Branch:       aws.String("develop"),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "develop", aws.ToString(updated.SyncConfiguration.Branch))

		repoSyncStatus, err := client.GetRepositorySyncStatus(
			ctx,
			&codeconnectionssdk.GetRepositorySyncStatusInput{
				RepositoryLinkId: aws.String(linkID),
				Branch:           aws.String("develop"),
				SyncType:         codeconnectionstypes.SyncConfigurationTypeCfnStackSync,
			},
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			codeconnectionstypes.RepositorySyncStatusSucceeded,
			repoSyncStatus.LatestSync.Status,
		)

		resourceSyncStatus, err := client.GetResourceSyncStatus(
			ctx,
			&codeconnectionssdk.GetResourceSyncStatusInput{
				ResourceName: aws.String("slice25-resource"),
				SyncType:     codeconnectionstypes.SyncConfigurationTypeCfnStackSync,
			},
		)
		require.NoError(t, err)
		require.NotNil(t, resourceSyncStatus.LatestSync)

		defs, err := client.ListRepositorySyncDefinitions(
			ctx,
			&codeconnectionssdk.ListRepositorySyncDefinitionsInput{
				RepositoryLinkId: aws.String(linkID),
				SyncType:         codeconnectionstypes.SyncConfigurationTypeCfnStackSync,
			},
		)
		require.NoError(t, err)
		require.Len(t, defs.RepositorySyncDefinitions, 1)
		assert.Equal(t, "slice25-resource", aws.ToString(defs.RepositorySyncDefinitions[0].Target))

		blocker, err := h.Backend.CreateSyncBlocker(
			ctx, "slice25-resource", string(codeconnectionstypes.SyncConfigurationTypeCfnStackSync),
			"AUTOMATED", "conflicting change",
		)
		require.NoError(t, err)

		summary, err := client.GetSyncBlockerSummary(
			ctx,
			&codeconnectionssdk.GetSyncBlockerSummaryInput{
				ResourceName: aws.String("slice25-resource"),
				SyncType:     codeconnectionstypes.SyncConfigurationTypeCfnStackSync,
			},
		)
		require.NoError(t, err)
		require.Len(t, summary.SyncBlockerSummary.LatestBlockers, 1)
		assert.Equal(t, blocker.ID, aws.ToString(summary.SyncBlockerSummary.LatestBlockers[0].Id))

		resolved, err := client.UpdateSyncBlocker(ctx, &codeconnectionssdk.UpdateSyncBlockerInput{
			Id:             aws.String(blocker.ID),
			ResolvedReason: aws.String("fixed"),
			ResourceName:   aws.String("slice25-resource"),
			SyncType:       codeconnectionstypes.SyncConfigurationTypeCfnStackSync,
		})
		require.NoError(t, err)
		assert.Equal(t, codeconnectionstypes.BlockerStatusResolved, resolved.SyncBlocker.Status)
	})
}
