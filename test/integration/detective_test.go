package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	detectivesdk "github.com/aws/aws-sdk-go-v2/service/detective"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createDetectiveClient returns a Detective client pointed at the shared test container.
func createDetectiveClient(t *testing.T) *detectivesdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return detectivesdk.NewFromConfig(cfg, func(o *detectivesdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_Detective_GraphLifecycle drives create→list→delete of a behavior graph.
func TestIntegration_Detective_GraphLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		tags map[string]string
		name string
	}{
		{name: "full_lifecycle", tags: map[string]string{"Environment": "test"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createDetectiveClient(t)

			createOut, err := client.CreateGraph(ctx, &detectivesdk.CreateGraphInput{
				Tags: tt.tags,
			})
			require.NoError(t, err, "CreateGraph should succeed")
			graphArn := aws.ToString(createOut.GraphArn)
			require.NotEmpty(t, graphArn, "graph ARN must be returned")

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteGraph(cleanupCtx, &detectivesdk.DeleteGraphInput{GraphArn: aws.String(graphArn)})
			})

			listOut, err := client.ListGraphs(ctx, &detectivesdk.ListGraphsInput{})
			require.NoError(t, err, "ListGraphs should succeed")

			found := false
			for _, g := range listOut.GraphList {
				if aws.ToString(g.Arn) == graphArn {
					found = true

					break
				}
			}

			assert.True(t, found, "created graph should appear in list")

			_, err = client.DeleteGraph(ctx, &detectivesdk.DeleteGraphInput{GraphArn: aws.String(graphArn)})
			require.NoError(t, err, "DeleteGraph should succeed")
		})
	}
}
