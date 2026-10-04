package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func TestResourceCreator_S3BucketNotificationConfiguration(t *testing.T) {
	t.Parallel()

	rules := map[string]any{"S3Key": map[string]any{"Rules": []any{
		map[string]any{"Name": "suffix", "Value": ".png"},
	}}}
	tests := []struct {
		config map[string]any
		name   string
		want   []string
	}{
		{
			name: "lambda",
			config: map[string]any{"LambdaConfigurations": []any{map[string]any{
				"Event":    "s3:ObjectCreated:*",
				"Function": "arn:aws:lambda:us-east-1:000000000000:function:f",
				"Filter":   rules,
			}}},
			want: []string{
				"<CloudFunctionConfiguration>",
				"<CloudFunction>arn:aws:lambda:us-east-1:000000000000:function:f",
				"<Event>s3:ObjectCreated:*</Event>",
				"<Name>suffix</Name><Value>.png</Value>",
			},
		},
		{
			name: "queue_and_topic",
			config: map[string]any{
				"QueueConfigurations": []any{map[string]any{
					"Event": "s3:ObjectRemoved:*", "Queue": "arn:aws:sqs:us-east-1:000000000000:q",
				}},
				"TopicConfigurations": []any{map[string]any{
					"Event": "s3:ObjectCreated:Put", "Topic": "arn:aws:sns:us-east-1:000000000000:t",
				}},
			},
			want: []string{"<QueueConfiguration>", "<Queue>arn:aws:sqs:us-east-1:000000000000:q</Queue>",
				"<TopicConfiguration>", "<Topic>arn:aws:sns:us-east-1:000000000000:t</Topic>"},
		},
		{
			name:   "eventbridge",
			config: map[string]any{"EventBridgeConfiguration": map[string]any{"EventBridgeEnabled": true}},
			want:   []string{"<EventBridgeConfiguration>"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends(t)
			rc := cloudformation.NewResourceCreator(backends)

			bucket, err := rc.Create(t.Context(), "B", "AWS::S3::Bucket",
				map[string]any{"NotificationConfiguration": tc.config}, nil, nil)
			require.NoError(t, err)

			got, err := backends.S3.Backend.GetBucketNotificationConfiguration(t.Context(), bucket)
			require.NoError(t, err)
			for _, w := range tc.want {
				assert.Contains(t, got, w)
			}
		})
	}
}
