package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	personalizesdk "github.com/aws/aws-sdk-go-v2/service/personalize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createPersonalizeClient returns a Personalize client pointed at the shared test container.
func createPersonalizeClient(t *testing.T) *personalizesdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return personalizesdk.NewFromConfig(cfg, func(o *personalizesdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_Personalize_DatasetGroupLifecycle drives create→describe→list→delete of a
// dataset group.
func TestIntegration_Personalize_DatasetGroupLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name      string
		groupName string
	}{
		{name: "full_lifecycle", groupName: "integ-group"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createPersonalizeClient(t)

			createOut, err := client.CreateDatasetGroup(ctx, &personalizesdk.CreateDatasetGroupInput{
				Name: aws.String(tt.groupName),
			})
			require.NoError(t, err, "CreateDatasetGroup should succeed")
			arn := aws.ToString(createOut.DatasetGroupArn)
			require.NotEmpty(t, arn, "dataset group ARN must be returned")

			t.Cleanup(func() {
				_, _ = client.DeleteDatasetGroup(ctx, &personalizesdk.DeleteDatasetGroupInput{
					DatasetGroupArn: aws.String(arn),
				})
			})

			descOut, err := client.DescribeDatasetGroup(ctx, &personalizesdk.DescribeDatasetGroupInput{
				DatasetGroupArn: aws.String(arn),
			})
			require.NoError(t, err, "DescribeDatasetGroup should succeed")
			require.NotNil(t, descOut.DatasetGroup)
			assert.Equal(t, tt.groupName, aws.ToString(descOut.DatasetGroup.Name))

			listOut, err := client.ListDatasetGroups(ctx, &personalizesdk.ListDatasetGroupsInput{})
			require.NoError(t, err, "ListDatasetGroups should succeed")

			found := false
			for _, g := range listOut.DatasetGroups {
				if aws.ToString(g.DatasetGroupArn) == arn {
					found = true

					break
				}
			}

			assert.True(t, found, "created dataset group should appear in list")

			_, err = client.DeleteDatasetGroup(ctx, &personalizesdk.DeleteDatasetGroupInput{
				DatasetGroupArn: aws.String(arn),
			})
			require.NoError(t, err, "DeleteDatasetGroup should succeed")
		})
	}
}
