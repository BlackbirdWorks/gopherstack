package sqs_test

import (
	"fmt"
	"testing"

	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSetQueueAttributes_EmptyValueUnsetsAttribute covers the fix in
// mergeQueueAttributes (queue_attributes.go): setting an optional
// JSON-policy-shaped attribute to the empty string must delete the key
// entirely, not store it with an empty value. Real SQS never returns such
// an attribute once unset -- it's simply absent from GetQueueAttributes.
//
// Leaving the key present (mapped to "") wedged the aws provider's SQS
// destroy waiter for aws_sqs_queue_redrive_policy: it polls
// GetQueueAttributes until the RedrivePolicy key disappears, which never
// happened, causing an infinite "Still destroying..." loop.
func TestSetQueueAttributes_EmptyValueUnsetsAttribute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buildValue func(dlqARN string) string
		name       string
		attrName   sqstypes.QueueAttributeName
		withDLQ    bool
	}{
		{
			name:     "redrive policy",
			attrName: sqstypes.QueueAttributeNameRedrivePolicy,
			withDLQ:  true,
			buildValue: func(dlqARN string) string {
				return fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":5}`, dlqARN)
			},
		},
		{
			name:     "policy",
			attrName: sqstypes.QueueAttributeNamePolicy,
			buildValue: func(string) string {
				return `{"Version":"2012-10-17","Statement":[]}`
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSQSClientForOversized(t)
			ctx := t.Context()

			var dlqARN string
			if tc.withDLQ {
				dlqOut, err := client.CreateQueue(ctx, &sqssdk.CreateQueueInput{
					QueueName: new("unset-attr-dlq"),
				})
				require.NoError(t, err)

				attrOut, err := client.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
					QueueUrl:       dlqOut.QueueUrl,
					AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
				})
				require.NoError(t, err)
				dlqARN = attrOut.Attributes["QueueArn"]
			}

			srcOut, err := client.CreateQueue(ctx, &sqssdk.CreateQueueInput{
				QueueName: new("unset-attr-src"),
			})
			require.NoError(t, err)

			_, err = client.SetQueueAttributes(ctx, &sqssdk.SetQueueAttributesInput{
				QueueUrl: srcOut.QueueUrl,
				Attributes: map[string]string{
					string(tc.attrName): tc.buildValue(dlqARN),
				},
			})
			require.NoError(t, err)

			afterSet, err := client.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
				QueueUrl:       srcOut.QueueUrl,
				AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
			})
			require.NoError(t, err)
			assert.Contains(t, afterSet.Attributes, string(tc.attrName))

			_, err = client.SetQueueAttributes(ctx, &sqssdk.SetQueueAttributesInput{
				QueueUrl: srcOut.QueueUrl,
				Attributes: map[string]string{
					string(tc.attrName): "",
				},
			})
			require.NoError(t, err)

			afterUnset, err := client.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
				QueueUrl:       srcOut.QueueUrl,
				AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
			})
			require.NoError(t, err)
			assert.NotContains(t, afterUnset.Attributes, string(tc.attrName),
				"unset attribute must be absent, not present with an empty value")
		})
	}
}
