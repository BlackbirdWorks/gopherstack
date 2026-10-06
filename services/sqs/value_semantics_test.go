package sqs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetQueueAttributes_PartialUpdateKeepsRest(t *testing.T) {
	t.Parallel()

	cases := []struct {
		set  map[string]string
		want map[string]string
		name string
	}{
		{
			name: "retention only",
			set:  map[string]string{"MessageRetentionPeriod": "120"},
			want: map[string]string{
				"MessageRetentionPeriod": "120",
				"VisibilityTimeout":      "45",
				"DelaySeconds":           "5",
				"MaximumMessageSize":     "2048",
			},
		},
		{
			name: "visibility only",
			set:  map[string]string{"VisibilityTimeout": "10"},
			want: map[string]string{
				"MessageRetentionPeriod": "345600",
				"VisibilityTimeout":      "10",
				"DelaySeconds":           "5",
				"MaximumMessageSize":     "2048",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newSQSTestServer(t)
			ctx := t.Context()

			q, err := client.CreateQueue(ctx, &sqssdk.CreateQueueInput{
				QueueName: aws.String("q"),
				Attributes: map[string]string{
					"VisibilityTimeout":  "45",
					"DelaySeconds":       "5",
					"MaximumMessageSize": "2048",
				},
			})
			require.NoError(t, err)

			_, err = client.SetQueueAttributes(
				ctx,
				&sqssdk.SetQueueAttributesInput{QueueUrl: q.QueueUrl, Attributes: tc.set},
			)
			require.NoError(t, err)

			got, err := client.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
				QueueUrl: q.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
			})
			require.NoError(t, err)

			for k, v := range tc.want {
				assert.Equal(t, v, got.Attributes[k], k)
			}
		})
	}
}
