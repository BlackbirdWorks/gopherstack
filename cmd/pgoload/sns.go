package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

const snsTopicName = "pgoload-topic"

// ensureSNSTopic creates snsTopicName (CreateTopic is idempotent by name)
// and subscribes it to the SQS queue at queueArn for realistic fan-out.
// Subscribing is best-effort: a failure there doesn't fail setup, since
// Publish/ListSubscriptions/GetTopicAttributes still exercise SNS on their
// own.
func ensureSNSTopic(ctx context.Context, cl *sns.Client, queueArn string, log *slog.Logger) (string, error) {
	out, err := cl.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(snsTopicName)})
	if err != nil {
		return "", fmt.Errorf("create topic %s: %w", snsTopicName, err)
	}

	topicArn := aws.ToString(out.TopicArn)

	if _, subErr := cl.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: aws.String(topicArn),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueArn),
	}); subErr != nil {
		log.WarnContext(ctx, "sns subscribe to sqs queue failed (continuing)", "error", subErr)
	}

	return topicArn, nil
}

// snsWorker repeatedly runs a mix of SNS operations, staggered by workerID,
// until ctx is done.
func snsWorker(ctx context.Context, cl *sns.Client, res *resources, workerID int, c *opCounter, log *slog.Logger) {
	ops := []opFunc{
		func(ctx context.Context, workerID, i int) error {
			return snsPublishOp(ctx, cl, res.topicArn, workerID, i)
		},
		func(ctx context.Context, workerID, i int) error {
			return snsPublishOp(ctx, cl, res.topicArn, workerID, i)
		},
		func(ctx context.Context, _, _ int) error { return snsListSubscriptionsOp(ctx, cl, res.topicArn) },
		func(ctx context.Context, _, _ int) error {
			return snsGetTopicAttributesOp(ctx, cl, res.topicArn)
		},
	}

	runOpLoop(ctx, workerID, ops, c, "sns", log)
}

func snsPublishOp(ctx context.Context, cl *sns.Client, topicArn string, workerID, i int) error {
	_, err := cl.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(topicArn),
		Message:  aws.String(fmt.Sprintf("pgoload notification worker=%d iter=%d", workerID, i)),
		Subject:  aws.String("pgoload"),
	})

	return err
}

func snsListSubscriptionsOp(ctx context.Context, cl *sns.Client, topicArn string) error {
	_, err := cl.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: aws.String(topicArn)})

	return err
}

func snsGetTopicAttributesOp(ctx context.Context, cl *sns.Client, topicArn string) error {
	_, err := cl.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(topicArn)})

	return err
}
