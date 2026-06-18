package integration_test

import (
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	rekognitionsdk "github.com/aws/aws-sdk-go-v2/service/rekognition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createRekognitionClient returns a Rekognition client pointed at the shared test container.
func createRekognitionClient(t *testing.T) *rekognitionsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return rekognitionsdk.NewFromConfig(cfg, func(o *rekognitionsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_Rekognition_CollectionLifecycle drives create→describe→list→delete of a
// face collection — the only stateful Rekognition resource.
func TestIntegration_Rekognition_CollectionLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name         string
		collectionID string
	}{
		{name: "full_lifecycle", collectionID: "integ-collection"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createRekognitionClient(t)

			createOut, err := client.CreateCollection(ctx, &rekognitionsdk.CreateCollectionInput{
				CollectionId: aws.String(tt.collectionID),
			})
			require.NoError(t, err, "CreateCollection should succeed")
			assert.NotEmpty(t, aws.ToString(createOut.CollectionArn), "collection ARN must be returned")

			t.Cleanup(func() {
				_, _ = client.DeleteCollection(ctx, &rekognitionsdk.DeleteCollectionInput{
					CollectionId: aws.String(tt.collectionID),
				})
			})

			descOut, err := client.DescribeCollection(ctx, &rekognitionsdk.DescribeCollectionInput{
				CollectionId: aws.String(tt.collectionID),
			})
			require.NoError(t, err, "DescribeCollection should succeed")
			assert.NotNil(t, descOut.FaceCount, "face count must be present")

			listOut, err := client.ListCollections(ctx, &rekognitionsdk.ListCollectionsInput{})
			require.NoError(t, err, "ListCollections should succeed")

			assert.True(
				t,
				slices.Contains(listOut.CollectionIds, tt.collectionID),
				"created collection should appear in list",
			)

			_, err = client.DeleteCollection(ctx, &rekognitionsdk.DeleteCollectionInput{
				CollectionId: aws.String(tt.collectionID),
			})
			require.NoError(t, err, "DeleteCollection should succeed")
		})
	}
}
