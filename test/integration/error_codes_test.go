package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	route53resolversdk "github.com/aws/aws-sdk-go-v2/service/route53resolver"
	r53rtypes "github.com/aws/aws-sdk-go-v2/service/route53resolver/types"
	schedulersdk "github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	swfsdk "github.com/aws/aws-sdk-go-v2/service/swf"
	swftypes "github.com/aws/aws-sdk-go-v2/service/swf/types"
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
		{
			name: "MalformedPolicyDocumentException_CreateRole",
			operation: func(t *testing.T) error {
				t.Helper()
				_, err := client.CreateRole(ctx, &iamsdk.CreateRoleInput{
					RoleName:                 aws.String("bad-role-" + uuid.NewString()[:8]),
					AssumeRolePolicyDocument: aws.String("not-json"),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var malformed *iamtypes.MalformedPolicyDocumentException
				assert.ErrorAs(t, err, &malformed, "expected MalformedPolicyDocumentException")
			},
		},
		{
			name: "DeleteConflictException_DeleteUserWithPolicy",
			operation: func(t *testing.T) error {
				t.Helper()
				userName := "conflict-user-" + uuid.NewString()[:8]
				_, err := client.CreateUser(ctx, &iamsdk.CreateUserInput{UserName: aws.String(userName)})
				require.NoError(t, err)

				polName := "conflict-pol-" + uuid.NewString()[:8]
				polOut, err := client.CreatePolicy(ctx, &iamsdk.CreatePolicyInput{
					PolicyName:     aws.String(polName),
					PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
				})
				require.NoError(t, err)

				t.Cleanup(func() {
					_, _ = client.DetachUserPolicy(ctx, &iamsdk.DetachUserPolicyInput{
						UserName:  aws.String(userName),
						PolicyArn: polOut.Policy.Arn,
					})
					_, _ = client.DeleteUser(ctx, &iamsdk.DeleteUserInput{UserName: aws.String(userName)})
					_, _ = client.DeletePolicy(ctx, &iamsdk.DeletePolicyInput{PolicyArn: polOut.Policy.Arn})
				})

				_, err = client.AttachUserPolicy(ctx, &iamsdk.AttachUserPolicyInput{
					UserName:  aws.String(userName),
					PolicyArn: polOut.Policy.Arn,
				})
				require.NoError(t, err)

				_, err = client.DeleteUser(ctx, &iamsdk.DeleteUserInput{UserName: aws.String(userName)})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var deleteConflict *iamtypes.DeleteConflictException
				assert.ErrorAs(t, err, &deleteConflict, "expected DeleteConflictException")
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
		{
			name: "InvalidKeyUsageException_Encrypt_SignVerifyKey",
			operation: func(t *testing.T) error {
				t.Helper()
				createOut, createErr := client.CreateKey(ctx, &kms.CreateKeyInput{
					Description: aws.String("test-sign-verify-" + uuid.NewString()[:8]),
					KeyUsage:    kmstypes.KeyUsageTypeSignVerify,
				})
				require.NoError(t, createErr)
				keyID := *createOut.KeyMetadata.KeyId

				_, err := client.Encrypt(ctx, &kms.EncryptInput{
					KeyId:     aws.String(keyID),
					Plaintext: []byte("test"),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var invalidKeyUsage *kmstypes.InvalidKeyUsageException
				assert.ErrorAs(t, err, &invalidKeyUsage, "expected InvalidKeyUsageException")
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

// TestIntegration_ErrorCodes_EventBridge verifies EventBridge error codes match AWS SDK types.
func TestIntegration_ErrorCodes_EventBridge(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createEventBridgeClient(t)
	ctx := t.Context()

	tests := []struct {
		operation func(t *testing.T) error
		check     func(t *testing.T, err error)
		name      string
	}{
		{
			name: "IllegalStatusException_DeleteDefaultEventBus",
			operation: func(t *testing.T) error {
				t.Helper()
				_, err := client.DeleteEventBus(ctx, &eventbridgesdk.DeleteEventBusInput{
					Name: aws.String("default"),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var illegalStatus *ebtypes.IllegalStatusException
				assert.ErrorAs(t, err, &illegalStatus, "expected IllegalStatusException")
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

// TestIntegration_ErrorCodes_SWF verifies SWF error codes match AWS SDK types.
func TestIntegration_ErrorCodes_SWF(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSWFClient(t)
	ctx := t.Context()

	tests := []struct {
		operation func(t *testing.T) error
		check     func(t *testing.T, err error)
		name      string
	}{
		{
			name: "DomainAlreadyExistsFault_RegisterDomain",
			operation: func(t *testing.T) error {
				t.Helper()
				domainName := "dup-domain-" + uuid.NewString()[:8]
				_, err := client.RegisterDomain(ctx, &swfsdk.RegisterDomainInput{
					Name:                                   aws.String(domainName),
					WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
				})
				require.NoError(t, err)

				_, err = client.RegisterDomain(ctx, &swfsdk.RegisterDomainInput{
					Name:                                   aws.String(domainName),
					WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var domainAlreadyExists *swftypes.DomainAlreadyExistsFault
				assert.ErrorAs(t, err, &domainAlreadyExists, "expected DomainAlreadyExistsFault")
			},
		},
		{
			name: "UnknownResourceFault_DeprecateDomain",
			operation: func(t *testing.T) error {
				t.Helper()
				_, err := client.DeprecateDomain(ctx, &swfsdk.DeprecateDomainInput{
					Name: aws.String("nonexistent-domain-" + uuid.NewString()[:8]),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var unknownResource *swftypes.UnknownResourceFault
				assert.ErrorAs(t, err, &unknownResource, "expected UnknownResourceFault")
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

// TestIntegration_ErrorCodes_Scheduler verifies Scheduler error codes match AWS SDK types.
func TestIntegration_ErrorCodes_Scheduler(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSchedulerClient(t)
	ctx := t.Context()

	tests := []struct {
		operation func(t *testing.T) error
		check     func(t *testing.T, err error)
		name      string
	}{
		{
			name: "ResourceNotFoundException_GetSchedule",
			operation: func(t *testing.T) error {
				t.Helper()
				_, err := client.GetSchedule(ctx, &schedulersdk.GetScheduleInput{
					Name: aws.String("nonexistent-schedule-" + uuid.NewString()[:8]),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var notFound *schedulertypes.ResourceNotFoundException
				assert.ErrorAs(t, err, &notFound, "expected ResourceNotFoundException")
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

// TestIntegration_ErrorCodes_Route53Resolver verifies that Route53Resolver error responses
// include a __type field so the SDK can dispatch to typed errors.
func TestIntegration_ErrorCodes_Route53Resolver(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createRoute53ResolverClient(t)
	ctx := t.Context()

	tests := []struct {
		operation func(t *testing.T) error
		check     func(t *testing.T, err error)
		name      string
	}{
		{
			name: "ResourceNotFoundException_GetResolverEndpoint",
			operation: func(t *testing.T) error {
				t.Helper()
				_, err := client.GetResolverEndpoint(ctx, &route53resolversdk.GetResolverEndpointInput{
					ResolverEndpointId: aws.String("nonexistent-endpoint-" + uuid.NewString()[:8]),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var notFound *r53rtypes.ResourceNotFoundException
				assert.ErrorAs(t, err, &notFound, "expected ResourceNotFoundException")
			},
		},
		{
			name: "ResourceNotFoundException_GetResolverRule",
			operation: func(t *testing.T) error {
				t.Helper()
				_, err := client.GetResolverRule(ctx, &route53resolversdk.GetResolverRuleInput{
					ResolverRuleId: aws.String("nonexistent-rule-" + uuid.NewString()[:8]),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var notFound *r53rtypes.ResourceNotFoundException
				assert.ErrorAs(t, err, &notFound, "expected ResourceNotFoundException")
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

// TestIntegration_ErrorCodes_RDS verifies that RDS XML error codes match AWS SDK typed errors.
func TestIntegration_ErrorCodes_RDS(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createRDSClient(t)
	ctx := t.Context()

	tests := []struct {
		operation func(t *testing.T) error
		check     func(t *testing.T, err error)
		name      string
	}{
		{
			name: "DBSubnetGroupNotFoundFault_DescribeDBSubnetGroups",
			operation: func(t *testing.T) error {
				t.Helper()
				_, err := client.DescribeDBSubnetGroups(ctx, &rdssdk.DescribeDBSubnetGroupsInput{
					DBSubnetGroupName: aws.String("nonexistent-sg-" + uuid.NewString()[:8]),
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var notFound *rdstypes.DBSubnetGroupNotFoundFault
				assert.ErrorAs(t, err, &notFound, "expected DBSubnetGroupNotFoundFault")
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
