package integration_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// snsEnvelope is the JSON body SNS delivers to a subscribed SQS queue
// (mirrors services/sqs/sns_delivery.go:snsEnvelope). Used here to assert
// on the actual decoded values a real subscriber would see, not just the
// presence of a substring.
type snsEnvelope struct {
	MessageAttributes map[string]struct {
		Type  string `json:"Type"`
		Value string `json:"Value"`
	} `json:"MessageAttributes"`
	MessageID string `json:"MessageId"`
	TopicArn  string `json:"TopicArn"`
	Message   string `json:"Message"`
}

// TestIntegration_SNS_AddRemovePermissionRoundTrip drives AddPermission and
// RemovePermission through a real client and reads the effect back via
// GetTopicAttributes's Policy field -- the only place a real client can
// observe an AddPermission grant. Neither op had ever been called by a
// typed client before this test.
func TestIntegration_SNS_AddRemovePermissionRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSNSClient(t)
	ctx := t.Context()

	topicName := "test-perm-" + uuid.NewString()
	topicOut, err := client.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(topicName)})
	require.NoError(t, err)
	topicARN := aws.ToString(topicOut.TopicArn)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteTopic(cleanupCtx, &sns.DeleteTopicInput{TopicArn: aws.String(topicARN)})
	})

	label := "allow-publish-" + uuid.NewString()[:8]

	_, err = client.AddPermission(ctx, &sns.AddPermissionInput{
		TopicArn:     aws.String(topicARN),
		Label:        aws.String(label),
		AWSAccountId: []string{"123456789012"},
		ActionName:   []string{"Publish"},
	})
	require.NoError(t, err)

	attrsOut, err := client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(topicARN)})
	require.NoError(t, err)

	var policy snsPolicyDocument
	require.NoError(t, json.Unmarshal([]byte(attrsOut.Attributes["Policy"]), &policy))

	var granted *snsPolicyStatement

	for i := range policy.Statement {
		if policy.Statement[i].Sid == label {
			granted = &policy.Statement[i]
		}
	}

	require.NotNil(t, granted, "AddPermission's statement should appear in GetTopicAttributes Policy")
	assert.Equal(t, "Allow", granted.Effect)
	assert.Equal(t, []string{"SNS:Publish"}, granted.Action)
	assert.Equal(t, []string{"arn:aws:iam::123456789012:root"}, granted.Principal.AWS)
	assert.Equal(t, topicARN, attrsOut.Attributes["TopicArn"])

	_, err = client.RemovePermission(ctx, &sns.RemovePermissionInput{
		TopicArn: aws.String(topicARN),
		Label:    aws.String(label),
	})
	require.NoError(t, err)

	attrsOut2, err := client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(topicARN)})
	require.NoError(t, err)

	var policyAfter snsPolicyDocument
	require.NoError(t, json.Unmarshal([]byte(attrsOut2.Attributes["Policy"]), &policyAfter))

	for _, stmt := range policyAfter.Statement {
		assert.NotEqual(t, label, stmt.Sid, "RemovePermission should drop the statement from Policy")
	}
}

// snsPolicyDocument and snsPolicyStatement decode the Policy attribute
// GetTopicAttributes returns (mirrors services/sns/topic_attributes.go's
// policyDocument/policyStatement, the shape AddPermission writes into it).
type snsPolicyDocument struct {
	Statement []snsPolicyStatement `json:"Statement"`
}

type snsPolicyStatement struct {
	Sid       string   `json:"Sid"`
	Effect    string   `json:"Effect"`
	Action    []string `json:"Action"`
	Principal struct {
		AWS []string `json:"AWS"`
	} `json:"Principal"`
}

// TestIntegration_SNS_TagResourceRoundTrip drives TagResource, ListTagsForResource
// and UntagResource through a real client and asserts the actual key/value pairs
// round-trip, not just presence of a 200.
func TestIntegration_SNS_TagResourceRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSNSClient(t)
	ctx := t.Context()

	topicName := "test-tags-" + uuid.NewString()
	topicOut, err := client.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(topicName)})
	require.NoError(t, err)
	topicARN := aws.ToString(topicOut.TopicArn)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteTopic(cleanupCtx, &sns.DeleteTopicInput{TopicArn: aws.String(topicARN)})
	})

	_, err = client.TagResource(ctx, &sns.TagResourceInput{
		ResourceArn: aws.String(topicARN),
		Tags: []snstypes.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
			{Key: aws.String("owner"), Value: aws.String("gopherstack")},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: aws.String(topicARN)})
	require.NoError(t, err)

	got := make(map[string]string, len(listOut.Tags))
	for _, tag := range listOut.Tags {
		got[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}

	assert.Equal(t, map[string]string{"env": "test", "owner": "gopherstack"}, got)

	_, err = client.UntagResource(ctx, &sns.UntagResourceInput{
		ResourceArn: aws.String(topicARN),
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: aws.String(topicARN)})
	require.NoError(t, err)

	got2 := make(map[string]string, len(listOut2.Tags))
	for _, tag := range listOut2.Tags {
		got2[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}

	assert.Equal(t, map[string]string{"owner": "gopherstack"}, got2, "UntagResource should drop only the removed key")
}

// TestIntegration_SNS_PublishBatchRoundTrip drives PublishBatch through a real
// client and reads every entry back off a subscribed SQS queue via a real SQS
// client, asserting each entry's body and message attribute survived the
// per-entry batch encoding (PublishBatch's per-entry MessageAttributes wire
// prefix was a previously-fixed bug class, see services/sns/handler_publish.go).
func TestIntegration_SNS_PublishBatchRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	snsClient := createSNSClient(t)
	sqsClient := createSQSClient(t)
	ctx := t.Context()

	topicName := "test-batch-" + uuid.NewString()
	topicOut, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(topicName)})
	require.NoError(t, err)
	topicARN := aws.ToString(topicOut.TopicArn)

	queueName := "test-batch-queue-" + uuid.NewString()
	queueOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(queueName)})
	require.NoError(t, err)
	queueURL := queueOut.QueueUrl

	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       queueURL,
		AttributeNames: []sqstypes.QueueAttributeName{"QueueArn"},
	})
	require.NoError(t, err)
	queueARN := attrOut.Attributes["QueueArn"]

	_, err = snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: aws.String(topicARN),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARN),
	})
	require.NoError(t, err)

	entries := []snstypes.PublishBatchRequestEntry{
		{
			Id:      aws.String("one"),
			Message: aws.String("batch-message-one-" + uuid.NewString()),
			MessageAttributes: map[string]snstypes.MessageAttributeValue{
				"priority": {DataType: aws.String("String"), StringValue: aws.String("high")},
			},
		},
		{
			Id:      aws.String("two"),
			Message: aws.String("batch-message-two-" + uuid.NewString()),
		},
	}

	batchOut, err := snsClient.PublishBatch(ctx, &sns.PublishBatchInput{
		TopicArn:                   aws.String(topicARN),
		PublishBatchRequestEntries: entries,
	})
	require.NoError(t, err)
	require.Empty(t, batchOut.Failed, "both batch entries should succeed")
	require.Len(t, batchOut.Successful, 2)

	wantBodies := map[string]string{
		aws.ToString(entries[0].Message): "high",
		aws.ToString(entries[1].Message): "",
	}

	got := make(map[string]string)

	for range 2 {
		recvOut, recvErr := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            queueURL,
			MaxNumberOfMessages: 2,
			WaitTimeSeconds:     2,
		})
		require.NoError(t, recvErr)

		for _, msg := range recvOut.Messages {
			var env snsEnvelope
			require.NoError(t, json.Unmarshal([]byte(aws.ToString(msg.Body)), &env))
			assert.Equal(t, topicARN, env.TopicArn)

			priority := ""
			if attr, ok := env.MessageAttributes["priority"]; ok {
				priority = attr.Value
			}

			got[env.Message] = priority
		}

		if len(got) == 2 {
			break
		}
	}

	assert.Equal(t, wantBodies, got, "each batch entry's body and attributes should survive delivery unchanged")
}
