package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_ECRAudit_DescribeImageReplicationStatus verifies that
// DescribeImageReplicationStatus computes one replication status entry per
// destination configured via PutReplicationConfiguration, rather than
// returning canned data.
func TestIntegration_ECRAudit_DescribeImageReplicationStatus(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECRClient(t)
	ctx := t.Context()

	repoName := "audit-repl-repo-" + uuid.NewString()[:8]

	// Create the repository.
	_, err := client.CreateRepository(ctx, &ecr.CreateRepositoryInput{
		RepositoryName: aws.String(repoName),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteRepository(ctx, &ecr.DeleteRepositoryInput{
			RepositoryName: aws.String(repoName),
			Force:          true,
		})
	})

	// Put an image into the repository.
	const imageTag = "v1"
	_, err = client.PutImage(ctx, &ecr.PutImageInput{
		RepositoryName: aws.String(repoName),
		ImageManifest:  aws.String(`{"schemaVersion":2}`),
		ImageTag:       aws.String(imageTag),
	})
	require.NoError(t, err)

	// Configure replication to two destination regions.  The AWS SDK requires
	// RegistryId on every destination, so the first uses the emulator's default
	// account ID and the second uses an explicit third-party account ID.
	const (
		destRegion1     = "us-west-2"
		destRegion2     = "eu-west-1"
		destRegistryID1 = "000000000000" // emulator default account ID
		destRegistryID2 = "210987654321"
	)

	_, err = client.PutReplicationConfiguration(ctx, &ecr.PutReplicationConfigurationInput{
		ReplicationConfiguration: &types.ReplicationConfiguration{
			Rules: []types.ReplicationRule{{
				Destinations: []types.ReplicationDestination{
					{Region: aws.String(destRegion1), RegistryId: aws.String(destRegistryID1)},
					{Region: aws.String(destRegion2), RegistryId: aws.String(destRegistryID2)},
				},
			}},
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		// Clear replication configuration so other tests are not affected.
		_, _ = client.PutReplicationConfiguration(ctx, &ecr.PutReplicationConfigurationInput{
			ReplicationConfiguration: &types.ReplicationConfiguration{Rules: []types.ReplicationRule{}},
		})
	})

	out, err := client.DescribeImageReplicationStatus(ctx, &ecr.DescribeImageReplicationStatusInput{
		RepositoryName: aws.String(repoName),
		ImageId:        &types.ImageIdentifier{ImageTag: aws.String(imageTag)},
	})
	require.NoError(t, err)
	require.NotNil(t, out)
	require.NotNil(t, out.RepositoryName)
	assert.Equal(t, repoName, *out.RepositoryName)

	// One status per configured destination.
	require.Len(t, out.ReplicationStatuses, 2)

	byRegion := map[string]types.ImageReplicationStatus{}
	for _, s := range out.ReplicationStatuses {
		require.NotNil(t, s.Region)
		assert.Equal(t, types.ReplicationStatusComplete, s.Status)
		require.NotNil(t, s.RegistryId)
		assert.NotEmpty(t, *s.RegistryId)
		byRegion[*s.Region] = s
	}

	require.Contains(t, byRegion, destRegion1)
	require.Contains(t, byRegion, destRegion2)
	// Each destination echoes the registry ID it was configured with.
	assert.Equal(t, destRegistryID1, *byRegion[destRegion1].RegistryId)
	assert.Equal(t, destRegistryID2, *byRegion[destRegion2].RegistryId)
}

// TestIntegration_ECRAudit_DescribeImageReplicationStatus_NoConfig verifies
// that with no replication configuration set, an empty replicationStatuses
// list is returned for an existing image.
func TestIntegration_ECRAudit_DescribeImageReplicationStatus_NoConfig(t *testing.T) {
	dumpContainerLogsOnFailure(t)

	client := createECRClient(t)
	ctx := t.Context()

	repoName := "audit-repl-noconf-" + uuid.NewString()[:8]

	_, err := client.CreateRepository(ctx, &ecr.CreateRepositoryInput{
		RepositoryName: aws.String(repoName),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteRepository(ctx, &ecr.DeleteRepositoryInput{
			RepositoryName: aws.String(repoName),
			Force:          true,
		})
	})

	const imageTag = "v1"
	_, err = client.PutImage(ctx, &ecr.PutImageInput{
		RepositoryName: aws.String(repoName),
		ImageManifest:  aws.String(`{"schemaVersion":2}`),
		ImageTag:       aws.String(imageTag),
	})
	require.NoError(t, err)

	// Ensure no replication configuration is set for this assertion.
	_, err = client.PutReplicationConfiguration(ctx, &ecr.PutReplicationConfigurationInput{
		ReplicationConfiguration: &types.ReplicationConfiguration{Rules: []types.ReplicationRule{}},
	})
	require.NoError(t, err)

	out, err := client.DescribeImageReplicationStatus(ctx, &ecr.DescribeImageReplicationStatusInput{
		RepositoryName: aws.String(repoName),
		ImageId:        &types.ImageIdentifier{ImageTag: aws.String(imageTag)},
	})
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Empty(t, out.ReplicationStatuses)
}

// TestIntegration_ECRAudit_DescribeImageReplicationStatus_Errors verifies the
// AWS-accurate error shapes for a missing image and a missing repository.
func TestIntegration_ECRAudit_DescribeImageReplicationStatus_Errors(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECRClient(t)
	ctx := t.Context()

	repoName := "audit-repl-err-" + uuid.NewString()[:8]

	_, err := client.CreateRepository(ctx, &ecr.CreateRepositoryInput{
		RepositoryName: aws.String(repoName),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteRepository(ctx, &ecr.DeleteRepositoryInput{
			RepositoryName: aws.String(repoName),
			Force:          true,
		})
	})

	// Existing repository, missing image -> ImageNotFoundException.
	_, err = client.DescribeImageReplicationStatus(ctx, &ecr.DescribeImageReplicationStatusInput{
		RepositoryName: aws.String(repoName),
		ImageId:        &types.ImageIdentifier{ImageTag: aws.String("does-not-exist")},
	})
	require.Error(t, err)
	var imageNotFound *types.ImageNotFoundException
	require.ErrorAs(t, err, &imageNotFound)

	// Missing repository -> RepositoryNotFoundException.
	_, err = client.DescribeImageReplicationStatus(ctx, &ecr.DescribeImageReplicationStatusInput{
		RepositoryName: aws.String("audit-repl-missing-" + uuid.NewString()[:8]),
		ImageId:        &types.ImageIdentifier{ImageTag: aws.String("v1")},
	})
	require.Error(t, err)
	var repoNotFound *types.RepositoryNotFoundException
	assert.ErrorAs(t, err, &repoNotFound)
}
