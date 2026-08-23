package opsworks_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opsworkssdk "github.com/aws/aws-sdk-go-v2/service/opsworks"
	"github.com/aws/aws-sdk-go-v2/service/opsworks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStackLifecycle_RoundTrip drives CreateStack, DescribeStacks,
// UpdateStack and DeleteStack through the real SDK client -- gopherstack-n3zi:
// opsworks measured 0/74 operations ever exercised by a typed client before
// this pass, so no prior test could have caught a wire-shape mismatch here.
func TestStackLifecycle_RoundTrip(t *testing.T) { //nolint:tparallel // sequential subtests
	t.Parallel()

	client := newTestClient(t)

	var stackID string

	//nolint:paralleltest // sequential by design: each step depends on backend state the previous step created
	t.Run("create", func(t *testing.T) {
		out, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
			Name:                      aws.String("web-tier"),
			Region:                    aws.String(rtTestRegion),
			DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
			ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
		})
		require.NoError(t, err)
		require.NotNil(t, out.StackId)
		stackID = aws.ToString(out.StackId)
	})

	//nolint:paralleltest // sequential by design
	t.Run("describe", func(t *testing.T) {
		out, err := client.DescribeStacks(t.Context(), &opsworkssdk.DescribeStacksInput{
			StackIds: []string{stackID},
		})
		require.NoError(t, err)
		require.Len(t, out.Stacks, 1)

		s := out.Stacks[0]
		assert.Equal(t, stackID, aws.ToString(s.StackId))
		assert.Equal(t, "web-tier", aws.ToString(s.Name))
		assert.Equal(t, rtTestRegion, aws.ToString(s.Region))
		assert.NotEmpty(t, aws.ToString(s.Arn))
		assert.NotEmpty(
			t,
			aws.ToString(s.CreatedAt),
			"CreatedAt is *string on the real wire, not a Timestamp -- an epoch/RFC3339 "+
				"mismatch would surface as a decode error here, not an empty field",
		)
	})

	//nolint:paralleltest // sequential by design
	t.Run("update", func(t *testing.T) {
		_, err := client.UpdateStack(t.Context(), &opsworkssdk.UpdateStackInput{
			StackId: aws.String(stackID),
			Name:    aws.String("web-tier-renamed"),
		})
		require.NoError(t, err)

		out, err := client.DescribeStacks(t.Context(), &opsworkssdk.DescribeStacksInput{
			StackIds: []string{stackID},
		})
		require.NoError(t, err)
		require.Len(t, out.Stacks, 1)
		assert.Equal(t, "web-tier-renamed", aws.ToString(out.Stacks[0].Name))
	})

	//nolint:paralleltest // sequential by design
	t.Run("delete", func(t *testing.T) {
		_, err := client.DeleteStack(t.Context(), &opsworkssdk.DeleteStackInput{
			StackId: aws.String(stackID),
		})
		require.NoError(t, err)

		out, err := client.DescribeStacks(t.Context(), &opsworkssdk.DescribeStacksInput{})
		require.NoError(t, err)
		assert.Empty(t, out.Stacks)
	})
}

// TestStackTags_RoundTrip drives TagResource, ListTags and UntagResource
// against a real stack ARN. gopherstack-n3zi: none of these three had ever
// been called by a typed client either.
func TestStackTags_RoundTrip(t *testing.T) { //nolint:tparallel // sequential subtests
	t.Parallel()

	client := newTestClient(t)

	created, createErr := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
		Name:                      aws.String("tagged-stack"),
		Region:                    aws.String(rtTestRegion),
		DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
		ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
	})
	require.NoError(t, createErr)

	described, describeErr := client.DescribeStacks(t.Context(), &opsworkssdk.DescribeStacksInput{
		StackIds: []string{aws.ToString(created.StackId)},
	})
	require.NoError(t, describeErr)
	require.Len(t, described.Stacks, 1)
	resourceARN := described.Stacks[0].Arn
	require.NotNil(t, resourceARN)

	//nolint:paralleltest // sequential by design: each step depends on tag state the previous step set
	t.Run("tag", func(t *testing.T) {
		_, err := client.TagResource(t.Context(), &opsworkssdk.TagResourceInput{
			ResourceArn: resourceARN,
			Tags:        map[string]string{"env": "prod", "team": "platform"},
		})
		require.NoError(t, err)
	})

	//nolint:paralleltest // sequential by design
	t.Run("list", func(t *testing.T) {
		out, err := client.ListTags(t.Context(), &opsworkssdk.ListTagsInput{
			ResourceArn: resourceARN,
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"env": "prod", "team": "platform"}, out.Tags)
	})

	//nolint:paralleltest // sequential by design
	t.Run("untag", func(t *testing.T) {
		_, err := client.UntagResource(t.Context(), &opsworkssdk.UntagResourceInput{
			ResourceArn: resourceARN,
			TagKeys:     []string{"team"},
		})
		require.NoError(t, err)

		out, err := client.ListTags(t.Context(), &opsworkssdk.ListTagsInput{
			ResourceArn: resourceARN,
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"env": "prod"}, out.Tags)
	})
}

// TestLayerLifecycle_RoundTrip drives CreateLayer, DescribeLayers and
// DeleteLayer -- also 0/74 before this pass.
func TestLayerLifecycle_RoundTrip(t *testing.T) { //nolint:tparallel // sequential subtests
	t.Parallel()

	client := newTestClient(t)

	stack, createErr := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
		Name:                      aws.String("layer-stack"),
		Region:                    aws.String(rtTestRegion),
		DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
		ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
	})
	require.NoError(t, createErr)
	stackID := aws.ToString(stack.StackId)

	var layerID string

	//nolint:paralleltest // sequential by design: each step depends on backend state the previous step created
	t.Run("create", func(t *testing.T) {
		out, err := client.CreateLayer(t.Context(), &opsworkssdk.CreateLayerInput{
			StackId:   aws.String(stackID),
			Type:      types.LayerTypeCustom,
			Name:      aws.String("app-layer"),
			Shortname: aws.String("applayer"),
		})
		require.NoError(t, err)
		require.NotNil(t, out.LayerId)
		layerID = aws.ToString(out.LayerId)
	})

	//nolint:paralleltest // sequential by design
	t.Run("describe", func(t *testing.T) {
		out, err := client.DescribeLayers(t.Context(), &opsworkssdk.DescribeLayersInput{
			StackId: aws.String(stackID),
		})
		require.NoError(t, err)
		require.Len(t, out.Layers, 1)
		assert.Equal(t, layerID, aws.ToString(out.Layers[0].LayerId))
		assert.Equal(t, "app-layer", aws.ToString(out.Layers[0].Name))
		assert.Equal(t, "applayer", aws.ToString(out.Layers[0].Shortname))
	})

	//nolint:paralleltest // sequential by design
	t.Run("delete", func(t *testing.T) {
		_, err := client.DeleteLayer(t.Context(), &opsworkssdk.DeleteLayerInput{
			LayerId: aws.String(layerID),
		})
		require.NoError(t, err)

		out, err := client.DescribeLayers(t.Context(), &opsworkssdk.DescribeLayersInput{
			StackId: aws.String(stackID),
		})
		require.NoError(t, err)
		assert.Empty(t, out.Layers)
	})
}
