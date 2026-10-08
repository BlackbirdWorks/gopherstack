package main

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudcontrol"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudControlDelegatesToServiceBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		patchedVal any
		verify     func(t *testing.T, fx *sfnFixture, id string, present bool)
		name       string
		typeName   string
		desired    string
		wantID     string
		patch      string
		patchedKey string
	}{
		{
			name: "s3_bucket", typeName: "AWS::S3::Bucket", desired: `{"BucketName":"cc-delegated-bucket"}`,
			wantID: "cc-delegated-bucket",
			patch:  `[{"op":"add","path":"/Tags","value":[{"Key":"env","Value":"test"}]}]`,
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true }).HeadBucket(
					t.Context(), &s3.HeadBucketInput{Bucket: aws.String(id)},
				)
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name: "sqs_queue", typeName: "AWS::SQS::Queue", desired: `{"QueueName":"cc-delegated-queue"}`,
			patch:      `[{"op":"add","path":"/VisibilityTimeout","value":77}]`,
			patchedKey: "VisibilityTimeout", patchedVal: float64(77),
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := sqs.NewFromConfig(fx.cfg).GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
					QueueUrl: aws.String(id),
				})
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name: "dynamodb_table", typeName: "AWS::DynamoDB::Table",
			desired: `{"TableName":"cc-delegated-table","BillingMode":"PROVISIONED",` +
				`"ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5},` +
				`"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],` +
				`"AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}`,
			wantID:     "cc-delegated-table",
			patch:      `[{"op":"replace","path":"/ProvisionedThroughput/ReadCapacityUnits","value":10}]`,
			patchedKey: "ProvisionedThroughput",
			patchedVal: map[string]any{"ReadCapacityUnits": 10, "WriteCapacityUnits": 5},
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := dynamodb.NewFromConfig(fx.cfg).DescribeTable(t.Context(), &dynamodb.DescribeTableInput{
					TableName: aws.String(id),
				})
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name: "log_group", typeName: "AWS::Logs::LogGroup",
			desired: `{"LogGroupName":"/cc/delegated","RetentionInDays":7}`, wantID: "/cc/delegated",
			patch:      `[{"op":"replace","path":"/RetentionInDays","value":30}]`,
			patchedKey: "RetentionInDays", patchedVal: float64(30),
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				out, err := cloudwatchlogs.NewFromConfig(fx.cfg).DescribeLogGroups(
					t.Context(), &cloudwatchlogs.DescribeLogGroupsInput{LogGroupNamePrefix: aws.String(id)},
				)
				require.NoError(t, err)
				assert.Equal(t, present, len(out.LogGroups) == 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			cc := cloudcontrol.NewFromConfig(fx.cfg)

			created, err := cc.CreateResource(t.Context(), &cloudcontrol.CreateResourceInput{
				TypeName: aws.String(tt.typeName), DesiredState: aws.String(tt.desired),
			})
			require.NoError(t, err)

			id := aws.ToString(created.ProgressEvent.Identifier)
			require.NotEmpty(t, id)

			if tt.wantID != "" {
				assert.Equal(t, tt.wantID, id)
			}

			tt.verify(t, fx, id, true)

			got, err := cc.GetResource(t.Context(), &cloudcontrol.GetResourceInput{
				TypeName: aws.String(tt.typeName), Identifier: aws.String(id),
			})
			require.NoError(t, err)
			assert.Equal(t, id, aws.ToString(got.ResourceDescription.Identifier))

			listed, err := cc.ListResources(t.Context(), &cloudcontrol.ListResourcesInput{
				TypeName: aws.String(tt.typeName),
			})
			require.NoError(t, err)
			require.Len(t, listed.ResourceDescriptions, 1)
			assert.Equal(t, id, aws.ToString(listed.ResourceDescriptions[0].Identifier))

			_, err = cc.UpdateResource(t.Context(), &cloudcontrol.UpdateResourceInput{
				TypeName: aws.String(tt.typeName), Identifier: aws.String(id), PatchDocument: aws.String(tt.patch),
			})
			require.NoError(t, err)

			if tt.patchedKey != "" {
				after, getErr := cc.GetResource(t.Context(), &cloudcontrol.GetResourceInput{
					TypeName: aws.String(tt.typeName), Identifier: aws.String(id),
				})
				require.NoError(t, getErr)
				assert.JSONEq(t, `{"`+tt.patchedKey+`":`+jsonScalar(tt.patchedVal)+`}`,
					subsetJSON(t, aws.ToString(after.ResourceDescription.Properties), tt.patchedKey))
			}

			_, err = cc.DeleteResource(t.Context(), &cloudcontrol.DeleteResourceInput{
				TypeName: aws.String(tt.typeName), Identifier: aws.String(id),
			})
			require.NoError(t, err)

			tt.verify(t, fx, id, false)

			_, err = cc.GetResource(t.Context(), &cloudcontrol.GetResourceInput{
				TypeName: aws.String(tt.typeName), Identifier: aws.String(id),
			})
			require.Error(t, err)
		})
	}
}

func jsonScalar(v any) string {
	b, _ := json.Marshal(v)

	return string(b)
}

func subsetJSON(t *testing.T, doc, key string) string {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(doc), &m))

	b, err := json.Marshal(map[string]any{key: m[key]})
	require.NoError(t, err)

	return string(b)
}
