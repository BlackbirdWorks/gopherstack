package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
)

const (
	ebRuleName   = "pgoload-rule"
	ebEventBus   = "default"
	ebSource     = "pgoload"
	ebDetailType = "pgoload.smoke"
)

// ensureEventBridgeRule creates ebRuleName (PutRule is an upsert, so this is
// safe to re-run) and targets it at the SQS queue behind queueArn.
func ensureEventBridgeRule(ctx context.Context, cl *eventbridge.Client, queueArn string, log *slog.Logger) error {
	if _, err := cl.PutRule(ctx, &eventbridge.PutRuleInput{
		Name:               aws.String(ebRuleName),
		ScheduleExpression: aws.String("rate(5 minutes)"),
	}); err != nil {
		return fmt.Errorf("put rule %s: %w", ebRuleName, err)
	}

	if _, err := cl.PutTargets(ctx, &eventbridge.PutTargetsInput{
		Rule: aws.String(ebRuleName),
		Targets: []ebtypes.Target{
			{Id: aws.String("pgoload-target"), Arn: aws.String(queueArn)},
		},
	}); err != nil {
		log.WarnContext(ctx, "eventbridge put targets failed (continuing)", "error", err)
	}

	return nil
}

// eventBridgeWorker repeatedly runs a mix of EventBridge operations,
// staggered by workerID, until ctx is done.
func eventBridgeWorker(ctx context.Context, cl *eventbridge.Client, workerID int, c *opCounter, log *slog.Logger) {
	ops := []opFunc{
		func(ctx context.Context, workerID, i int) error { return ebPutEventsOp(ctx, cl, workerID, i) },
		func(ctx context.Context, workerID, i int) error { return ebPutEventsOp(ctx, cl, workerID, i) },
		func(ctx context.Context, workerID, i int) error { return ebListRulesOp(ctx, cl) },
		func(ctx context.Context, workerID, i int) error { return ebDescribeRuleOp(ctx, cl) },
		func(ctx context.Context, workerID, i int) error { return ebListTargetsByRuleOp(ctx, cl) },
	}

	runOpLoop(ctx, workerID, ops, c, "eventbridge", log)
}

func ebPutEventsOp(ctx context.Context, cl *eventbridge.Client, workerID, i int) error {
	_, err := cl.PutEvents(ctx, &eventbridge.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{
			{
				EventBusName: aws.String(ebEventBus),
				Source:       aws.String(ebSource),
				DetailType:   aws.String(ebDetailType),
				Detail:       aws.String(fmt.Sprintf(`{"worker":%d,"iter":%d}`, workerID, i)),
			},
		},
	})

	return err
}

func ebListRulesOp(ctx context.Context, cl *eventbridge.Client) error {
	_, err := cl.ListRules(ctx, &eventbridge.ListRulesInput{})

	return err
}

func ebDescribeRuleOp(ctx context.Context, cl *eventbridge.Client) error {
	_, err := cl.DescribeRule(ctx, &eventbridge.DescribeRuleInput{Name: aws.String(ebRuleName)})

	return err
}

func ebListTargetsByRuleOp(ctx context.Context, cl *eventbridge.Client) error {
	_, err := cl.ListTargetsByRule(ctx, &eventbridge.ListTargetsByRuleInput{Rule: aws.String(ebRuleName)})

	return err
}
