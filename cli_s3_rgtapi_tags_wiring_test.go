package main

import (
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

// TestInitializeServices_S3TagsWiring drives the actual composition root
// (initializeServices, the function cli.go's Run() calls) rather than
// invoking wireTaggingS3 directly, so that deleting the wiring call from
// wireResourceGroupsTaggingSweep5 -- not just breaking the helper function
// itself -- is what this test is sensitive to. This is the shape
// iot/appsync/sagemaker's undetected routing gaps had: correct code nothing
// reaches (gopherstack-2mwl), and specifically the bug gopherstack-8kco
// tracked for s3/s3control.
func TestInitializeServices_S3TagsWiring(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000"}
	appCtx := &service.AppContext{
		Logger:     slog.Default(),
		Config:     cli,
		JanitorCtx: t.Context(),
	}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(appCtx)
	require.NoError(t, err)

	byName := serviceByName(services)

	s3H, ok := byName["S3"].(*s3backend.S3Handler)
	require.True(t, ok, "S3 handler must be registered")

	s3Bk, ok := s3H.Backend.(*s3backend.InMemoryBackend)
	require.True(t, ok, "S3 backend must be an InMemoryBackend")

	rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
	require.True(t, ok, "ResourceGroupsTaggingAPI handler must be registered")

	ctx := t.Context()
	bucketName := "wiring-test-bucket"

	_, err = s3Bk.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	err = s3Bk.PutBucketTagging(ctx, bucketName, []types.Tag{
		{Key: aws.String("Team"), Value: aws.String("emu")},
	})
	require.NoError(t, err)

	out, err := rgtH.Backend.GetResources(ctx, &rgtapibackend.GetResourcesInput{
		TagFilters: []rgtapibackend.TagFilter{
			{Key: "Team", Values: []string{"emu"}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out)

	wantARN := "arn:aws:s3:::" + bucketName

	gotARNs := make([]string, 0, len(out.ResourceTagMappingList))
	for _, m := range out.ResourceTagMappingList {
		gotARNs = append(gotARNs, m.ResourceARN)
	}

	assert.Contains(t, gotARNs, wantARN,
		"GetResources must see the tagged S3 bucket through the actual cli.go "+
			"composition root, not just the wiring helper called directly")
}
