package resourcegroupstaggingapi_test

import (
	"context"
	"maps"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rgtasdk "github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// TestRealClient_TagsAndCompliance drives resourcegroupstaggingapi's
// typed-coverage-blind ops (gopherstack-n3zi) through the real
// aws-sdk-go-v2 client.
func TestRealClient_TagsAndCompliance(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "tag keys and values via a registered provider",
			run: func(t *testing.T) {
				t.Helper()

				b := resourcegroupstaggingapi.NewInMemoryBackend("123456789012", "us-east-1")
				const s15ARN = "arn:aws:s3:::s15-bucket"
				b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource {
					return []resourcegroupstaggingapi.TaggedResource{
						{
							ResourceARN:  s15ARN,
							ResourceType: "s3:bucket",
							Tags:         map[string]string{"env": "test", "team": "platform"},
						},
					}
				})
				client := newTestRGTAClient(t, b)
				ctx := t.Context()

				keysOut, err := client.GetTagKeys(ctx, &rgtasdk.GetTagKeysInput{})
				require.NoError(t, err)
				assert.Contains(t, keysOut.TagKeys, "env")
				assert.Contains(t, keysOut.TagKeys, "team")

				valuesOut, err := client.GetTagValues(ctx, &rgtasdk.GetTagValuesInput{Key: aws.String("env")})
				require.NoError(t, err)
				assert.Contains(t, valuesOut.TagValues, "test")
			},
		},
		{
			name: "tag and untag resources",
			run: func(t *testing.T) {
				t.Helper()

				b := resourcegroupstaggingapi.NewInMemoryBackend("123456789012", "us-east-1")
				tagged := map[string]map[string]string{}
				b.RegisterARNTagger(func(_ context.Context, arn string, tags map[string]string) (bool, error) {
					if arn != "arn:aws:s3:::s15-tag-bucket" {
						return false, nil
					}
					if tagged[arn] == nil {
						tagged[arn] = map[string]string{}
					}
					maps.Copy(tagged[arn], tags)

					return true, nil
				})
				b.RegisterARNUntagger(func(_ context.Context, arn string, keys []string) (bool, error) {
					if arn != "arn:aws:s3:::s15-tag-bucket" {
						return false, nil
					}
					for _, k := range keys {
						delete(tagged[arn], k)
					}

					return true, nil
				})
				client := newTestRGTAClient(t, b)
				ctx := t.Context()

				tagOut, err := client.TagResources(ctx, &rgtasdk.TagResourcesInput{
					ResourceARNList: []string{"arn:aws:s3:::s15-tag-bucket", "arn:aws:s3:::s15-unhandled-bucket"},
					Tags:            map[string]string{"env": "test"},
				})
				require.NoError(t, err)
				assert.NotContains(t, tagOut.FailedResourcesMap, "arn:aws:s3:::s15-tag-bucket")
				require.Contains(t, tagOut.FailedResourcesMap, "arn:aws:s3:::s15-unhandled-bucket")
				assert.EqualValues(t, "InvalidParameterException",
					tagOut.FailedResourcesMap["arn:aws:s3:::s15-unhandled-bucket"].ErrorCode)
				assert.Equal(t, "test", tagged["arn:aws:s3:::s15-tag-bucket"]["env"])

				untagOut, err := client.UntagResources(ctx, &rgtasdk.UntagResourcesInput{
					ResourceARNList: []string{"arn:aws:s3:::s15-tag-bucket"},
					TagKeys:         []string{"env"},
				})
				require.NoError(t, err)
				assert.Empty(t, untagOut.FailedResourcesMap)
				assert.NotContains(t, tagged["arn:aws:s3:::s15-tag-bucket"], "env")
			},
		},
		{
			name: "compliance summary, required tags, and report creation",
			run: func(t *testing.T) {
				t.Helper()

				b := resourcegroupstaggingapi.NewInMemoryBackend("123456789012", "us-east-1")
				client := newTestRGTAClient(t, b)
				ctx := t.Context()

				complianceOut, err := client.GetComplianceSummary(ctx, &rgtasdk.GetComplianceSummaryInput{})
				require.NoError(t, err)
				assert.NotNil(t, complianceOut.SummaryList)

				requiredOut, err := client.ListRequiredTags(ctx, &rgtasdk.ListRequiredTagsInput{})
				require.NoError(t, err)
				assert.Empty(t, requiredOut.RequiredTags)

				_, err = client.StartReportCreation(ctx, &rgtasdk.StartReportCreationInput{
					S3Bucket: aws.String("s15-report-bucket"),
				})
				require.NoError(t, err)

				descOut, err := client.DescribeReportCreation(ctx, &rgtasdk.DescribeReportCreationInput{})
				require.NoError(t, err)
				assert.NotEmpty(t, aws.ToString(descOut.Status))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
