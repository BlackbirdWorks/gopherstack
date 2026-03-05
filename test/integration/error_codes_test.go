package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_ErrorCodes_IAM verifies that IAM error codes exactly match AWS SDK types.
func TestIntegration_ErrorCodes_IAM(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	tests := []struct {
		operation func(t *testing.T) error
		check     func(t *testing.T, err error)
		name      string
	}{
		{
			name: "NoSuchEntityException_GetUser",
			operation: func(t *testing.T) error {
				t.Helper()
				_, err := client.GetUser(ctx, &iamsdk.GetUserInput{
					UserName: aws.String("nonexistent-user-" + uuid.NewString()[:8]),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var noSuchEntity *iamtypes.NoSuchEntityException
				assert.ErrorAs(t, err, &noSuchEntity, "expected NoSuchEntityException")
			},
		},
		{
			name: "EntityAlreadyExistsException_CreateUser",
			operation: func(t *testing.T) error {
				t.Helper()
				userName := "dup-user-" + uuid.NewString()[:8]
				_, err := client.CreateUser(ctx, &iamsdk.CreateUserInput{UserName: aws.String(userName)})
				require.NoError(t, err)
				_, err = client.CreateUser(ctx, &iamsdk.CreateUserInput{UserName: aws.String(userName)})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var alreadyExists *iamtypes.EntityAlreadyExistsException
				assert.ErrorAs(t, err, &alreadyExists, "expected EntityAlreadyExistsException")
			},
		},
		{
			name: "NoSuchEntityException_GetRole",
			operation: func(t *testing.T) error {
				t.Helper()
				_, err := client.GetRole(ctx, &iamsdk.GetRoleInput{
					RoleName: aws.String("nonexistent-role-" + uuid.NewString()[:8]),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var noSuchEntity *iamtypes.NoSuchEntityException
				assert.ErrorAs(t, err, &noSuchEntity, "expected NoSuchEntityException")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.operation(t)
			tt.check(t, err)
		})
	}
}

// TestIntegration_ErrorCodes_SNS verifies that SNS error codes exactly match AWS SDK types.
func TestIntegration_ErrorCodes_SNS(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSNSClient(t)
	ctx := t.Context()

	tests := []struct {
		operation func(t *testing.T) error
		check     func(t *testing.T, err error)
		name      string
	}{
		{
			name: "NotFoundException_DeleteTopic",
			operation: func(t *testing.T) error {
				t.Helper()
				_, err := client.DeleteTopic(ctx, &snssdk.DeleteTopicInput{
					TopicArn: aws.String(
						"arn:aws:sns:us-east-1:000000000000:nonexistent-topic-" + uuid.NewString()[:8],
					),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var notFound *snstypes.NotFoundException
				assert.ErrorAs(t, err, &notFound, "expected NotFoundException")
			},
		},
		{
			name: "NotFoundException_GetTopicAttributes",
			operation: func(t *testing.T) error {
				t.Helper()
				_, err := client.GetTopicAttributes(ctx, &snssdk.GetTopicAttributesInput{
					TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:nonexistent-" + uuid.NewString()[:8]),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var notFound *snstypes.NotFoundException
				assert.ErrorAs(t, err, &notFound, "expected NotFoundException")
			},
		},
		{
			name: "NotFoundException_Unsubscribe",
			operation: func(t *testing.T) error {
				t.Helper()
				_, err := client.Unsubscribe(ctx, &snssdk.UnsubscribeInput{
					SubscriptionArn: aws.String(
						"arn:aws:sns:us-east-1:000000000000:nonexistent:sub-" + uuid.NewString()[:8],
					),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var notFound *snstypes.NotFoundException
				assert.ErrorAs(t, err, &notFound, "expected NotFoundException")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.operation(t)
			tt.check(t, err)
		})
	}
}

// TestIntegration_ErrorCodes_KMS verifies that KMS error codes exactly match AWS SDK types.
func TestIntegration_ErrorCodes_KMS(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	tests := []struct {
		operation func(t *testing.T) error
		check     func(t *testing.T, err error)
		name      string
	}{
		{
			name: "NotFoundException_DescribeKey",
			operation: func(t *testing.T) error {
				t.Helper()
				_, err := client.DescribeKey(ctx, &kms.DescribeKeyInput{
					KeyId: aws.String("00000000-0000-0000-0000-000000000000"),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var notFound *kmstypes.NotFoundException
				assert.ErrorAs(t, err, &notFound, "expected NotFoundException")
			},
		},
		{
			name: "DisabledException_Encrypt",
			operation: func(t *testing.T) error {
				t.Helper()
				createOut, createErr := client.CreateKey(ctx, &kms.CreateKeyInput{
					Description: aws.String("test-disabled-" + uuid.NewString()[:8]),
				})
				require.NoError(t, createErr)
				keyID := *createOut.KeyMetadata.KeyId

				_, disableErr := client.DisableKey(ctx, &kms.DisableKeyInput{KeyId: aws.String(keyID)})
				require.NoError(t, disableErr)

				_, err := client.Encrypt(ctx, &kms.EncryptInput{
					KeyId:     aws.String(keyID),
					Plaintext: []byte("test"),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var disabled *kmstypes.DisabledException
				assert.ErrorAs(t, err, &disabled, "expected DisabledException")
			},
		},
		{
			name: "KMSInvalidStateException_EncryptPendingDeletion",
			operation: func(t *testing.T) error {
				t.Helper()
				createOut, createErr := client.CreateKey(ctx, &kms.CreateKeyInput{
					Description: aws.String("test-pending-deletion-" + uuid.NewString()[:8]),
				})
				require.NoError(t, createErr)
				keyID := *createOut.KeyMetadata.KeyId

				_, schedErr := client.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{
					KeyId:               aws.String(keyID),
					PendingWindowInDays: aws.Int32(7),
				})
				require.NoError(t, schedErr)

				_, err := client.Encrypt(ctx, &kms.EncryptInput{
					KeyId:     aws.String(keyID),
					Plaintext: []byte("test"),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var invalidState *kmstypes.KMSInvalidStateException
				assert.ErrorAs(t, err, &invalidState, "expected KMSInvalidStateException")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.operation(t)
			tt.check(t, err)
		})
	}
}

// TestIntegration_ErrorCodes_SQS verifies that SQS error codes exactly match AWS SDK types.
func TestIntegration_ErrorCodes_SQS(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSQSClient(t)
	ctx := t.Context()

	tests := []struct {
		operation func(t *testing.T) error
		check     func(t *testing.T, err error)
		name      string
	}{
		{
			name: "QueueDoesNotExist_GetQueueUrl",
			operation: func(t *testing.T) error {
				t.Helper()
				_, err := client.GetQueueUrl(ctx, &sqssdk.GetQueueUrlInput{
					QueueName: aws.String("nonexistent-queue-" + uuid.NewString()[:8]),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var queueDoesNotExist *sqstypes.QueueDoesNotExist
				assert.ErrorAs(t, err, &queueDoesNotExist, "expected QueueDoesNotExist")
			},
		},
		{
			name: "QueueNameExists_CreateQueue",
			operation: func(t *testing.T) error {
				t.Helper()
				qName := "dup-queue-" + uuid.NewString()[:8]
				_, err := client.CreateQueue(ctx, &sqssdk.CreateQueueInput{
					QueueName: aws.String(qName),
				})
				require.NoError(t, err)
				// Create same queue again with different attributes to trigger QueueNameExists
				_, err = client.CreateQueue(ctx, &sqssdk.CreateQueueInput{
					QueueName: aws.String(qName),
					Attributes: map[string]string{
						"VisibilityTimeout": "60",
					},
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var queueNameExists *sqstypes.QueueNameExists
				assert.ErrorAs(t, err, &queueNameExists, "expected QueueNameExists")
			},
		},
		{
			name: "MessageNotInflight_ChangeMessageVisibility",
			operation: func(t *testing.T) error {
				t.Helper()
				qName := "inflight-queue-" + uuid.NewString()[:8]
				createOut, createErr := client.CreateQueue(ctx, &sqssdk.CreateQueueInput{
					QueueName: aws.String(qName),
				})
				require.NoError(t, createErr)
				qURL := *createOut.QueueUrl

				_, err := client.ChangeMessageVisibility(ctx, &sqssdk.ChangeMessageVisibilityInput{
					QueueUrl:          aws.String(qURL),
					ReceiptHandle:     aws.String("invalid-non-inflight-receipt"),
					VisibilityTimeout: 30,
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var notInflight *sqstypes.MessageNotInflight
				assert.ErrorAs(t, err, &notInflight, "expected MessageNotInflight")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.operation(t)
			tt.check(t, err)
		})
	}
}
