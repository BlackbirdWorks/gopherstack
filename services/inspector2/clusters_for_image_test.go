package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ecs"
	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

type ecsSibling struct{ h *ecs.Handler }

func (s ecsSibling) GetECSHandler() service.Registerable { return s.h }

func TestGetClustersForImage_ECS(t *testing.T) {
	t.Parallel()

	const digest = "sha256:0123abcd"

	ecsBackend := ecs.NewInMemoryBackend(rtTestAccountID, rtTestRegion, ecs.NewNoopRunner())
	ecsHandler := ecs.NewHandler(ecsBackend)

	cluster, err := ecsBackend.CreateCluster(ecs.CreateClusterInput{ClusterName: "prod"})
	require.NoError(t, err)

	for _, td := range []struct{ family, image string }{
		{"uses-image", "123456789012.dkr.ecr.us-east-1.amazonaws.com/app@" + digest},
		{"other-image", "123456789012.dkr.ecr.us-east-1.amazonaws.com/app@sha256:ffff"},
	} {
		_, tdErr := ecsBackend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
			Family:               td.family,
			ContainerDefinitions: []ecs.ContainerDefinition{{Name: "c", Image: td.image}},
		})
		require.NoError(t, tdErr)

		_, _, runErr := ecsBackend.RunTask(
			ecs.RunTaskInput{Cluster: "prod", TaskDefinition: td.family, Group: "family:" + td.family},
		)
		require.NoError(t, runErr)
	}

	backend := inspector2.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	backend.SetAppConfig(ecsSibling{h: ecsHandler})
	client := newRoundTripClient(t, inspector2.NewHandler(backend))

	out, err := client.GetClustersForImage(t.Context(), &inspector2sdk.GetClustersForImageInput{
		Filter: &types.ClusterForImageFilterCriteria{ResourceId: aws.String(digest)},
	})
	require.NoError(t, err)
	require.Len(t, out.Cluster, 1)
	assert.Equal(t, cluster.ClusterArn, aws.ToString(out.Cluster[0].ClusterArn))
	require.Len(t, out.Cluster[0].ClusterDetails, 1)

	d := out.Cluster[0].ClusterDetails[0]
	assert.EqualValues(t, 1, aws.ToInt64(d.RunningUnitCount))
	assert.EqualValues(t, 0, aws.ToInt64(d.StoppedUnitCount))

	ecsMeta, ok := d.ClusterMetadata.(*types.ClusterMetadataMemberAwsEcsMetadataDetails)
	require.True(t, ok)
	assert.Equal(t, "family:uses-image", aws.ToString(ecsMeta.Value.DetailsGroup))
	assert.Contains(t, aws.ToString(ecsMeta.Value.TaskDefinitionArn), "uses-image")

	none, err := client.GetClustersForImage(t.Context(), &inspector2sdk.GetClustersForImageInput{
		Filter: &types.ClusterForImageFilterCriteria{ResourceId: aws.String("sha256:unknown")},
	})
	require.NoError(t, err)
	assert.Empty(t, none.Cluster)
}
