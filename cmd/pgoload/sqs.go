package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	sqsQueueName = "pgoload-queue"

	sqsBodySmallSize  = 128
	sqsBodyMediumSize = 2048
	// sqsBodySizeMod: every sqsBodySizeMod'th message gets the larger body.
	sqsBodySizeMod = 5

	sqsBatchSize          = 5
	sqsReceiveMaxMessages = 10
	sqsReceiveWaitSeconds = 1
)

// ensureSQSQueue creates sqsQueueName, tolerating one that already exists,
// and returns its URL and ARN.
func ensureSQSQueue(ctx context.Context, cl *sqs.Client, log *slog.Logger) (string, string, error) {
	out, err := cl.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(sqsQueueName)})
	if err != nil {
		return "", "", fmt.Errorf("create queue %s: %w", sqsQueueName, err)
	}

	log.InfoContext(ctx, "sqs queue ready", "queue", sqsQueueName)

	queueURL := aws.ToString(out.QueueUrl)

	attrs, err := cl.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return "", "", fmt.Errorf("get queue arn %s: %w", sqsQueueName, err)
	}

	return queueURL, attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)], nil
}

func sqsBody(i int) string {
	size := sqsBodySmallSize
	if i%sqsBodySizeMod == 0 {
		size = sqsBodyMediumSize
	}

	return strings.Repeat("m", size)
}

// sqsWorker repeatedly runs a mix of SQS operations, staggered by workerID,
// until ctx is done.
func sqsWorker(ctx context.Context, cl *sqs.Client, res *resources, workerID int, c *opCounter, log *slog.Logger) {
	ops := []opFunc{
		func(ctx context.Context, _, i int) error { return sqsSendMessageOp(ctx, cl, res.queueURL, i) },
		func(ctx context.Context, _, i int) error { return sqsSendMessageOp(ctx, cl, res.queueURL, i) },
		func(ctx context.Context, _, i int) error {
			return sqsSendMessageBatchOp(ctx, cl, res.queueURL, i)
		},
		func(ctx context.Context, _, _ int) error { return sqsReceiveDeleteOp(ctx, cl, res.queueURL) },
		func(ctx context.Context, _, _ int) error {
			return sqsGetQueueAttributesOp(ctx, cl, res.queueURL)
		},
	}

	runOpLoop(ctx, workerID, ops, c, "sqs", log)
}

func sqsSendMessageOp(ctx context.Context, cl *sqs.Client, queueURL string, i int) error {
	_, err := cl.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(sqsBody(i)),
	})

	return err
}

func sqsSendMessageBatchOp(ctx context.Context, cl *sqs.Client, queueURL string, base int) error {
	entries := make([]sqstypes.SendMessageBatchRequestEntry, 0, sqsBatchSize)
	for j := range sqsBatchSize {
		entries = append(entries, sqstypes.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprintf("m%d", j)),
			MessageBody: aws.String(sqsBody(base + j)),
		})
	}

	_, err := cl.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(queueURL),
		Entries:  entries,
	})

	return err
}

// sqsReceiveDeleteOp receives a batch of messages and deletes each one, so
// the receive and delete wire paths are exercised together like a real
// consumer.
func sqsReceiveDeleteOp(ctx context.Context, cl *sqs.Client, queueURL string) error {
	out, err := cl.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: sqsReceiveMaxMessages,
		WaitTimeSeconds:     sqsReceiveWaitSeconds,
	})
	if err != nil {
		return err
	}

	for _, msg := range out.Messages {
		if _, delErr := cl.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueURL),
			ReceiptHandle: msg.ReceiptHandle,
		}); delErr != nil {
			return fmt.Errorf("delete message: %w", delErr)
		}
	}

	return nil
}

func sqsGetQueueAttributesOp(ctx context.Context, cl *sqs.Client, queueURL string) error {
	_, err := cl.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
	})

	return err
}
