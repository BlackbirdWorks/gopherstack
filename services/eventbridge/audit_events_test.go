package eventbridge_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// ---------------------------------------------------------------------------
// Scheduled rule target delivery (bug fix: was skipped in buildDeliveryPlan)
// ---------------------------------------------------------------------------

func TestAuditEvents_ScheduledRuleDeliversToSQSTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		schedExpr        string
		wantDetailType   string
		wantSourcePrefix string
	}{
		{
			name:             "rate_rule_delivers_to_target",
			schedExpr:        "rate(1 minute)",
			wantDetailType:   "Scheduled Event",
			wantSourcePrefix: "aws.events",
		},
		{
			name:             "cron_rule_delivers_with_cron_detail_type",
			schedExpr:        "cron(0 12 * * ? *)",
			wantDetailType:   "Scheduled Event (cron)",
			wantSourcePrefix: "aws.events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sqsMock := newMockSQSSender()
			b := setupDeliveryBackend(t, sqsMock, newMockLambdaInvoker())
			const (
				queueARN = "arn:aws:sqs:us-east-1:123456789012:sched-queue"
				ruleName = "sched-rule"
			)

			_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
				Name:               ruleName,
				ScheduleExpression: tc.schedExpr,
				State:              "ENABLED",
			})
			require.NoError(t, err)

			rule, err := b.DescribeRule(context.Background(), ruleName, "default")
			require.NoError(t, err)

			_, err = b.PutTargets(context.Background(), ruleName, "default", []eventbridge.Target{
				{ID: "t1", Arn: queueARN},
			})
			require.NoError(t, err)

			sched := eventbridge.NewScheduler(b, 0)
			// Use a tick far in the future so any schedule expression has a next
			// fire time before it, regardless of the current wall-clock time.
			tick := time.Now().Add(25 * time.Hour)
			lastFired := map[string]time.Time{
				rule.Arn: tick.Add(-25 * time.Hour),
			}
			sched.ProcessTickForTest(t.Context(), tick, lastFired)

			msgs := sqsMock.MessagesFor(queueARN)
			require.Len(t, msgs, 1, "expected exactly one message delivered to SQS target")
			assert.Contains(t, msgs[0], tc.wantSourcePrefix)
			assert.Contains(t, msgs[0], tc.wantDetailType)
		})
	}
}

func TestAuditEvents_DisabledScheduledRuleDoesNotDeliver(t *testing.T) {
	t.Parallel()

	sqsMock := newMockSQSSender()
	b := setupDeliveryBackend(t, sqsMock, newMockLambdaInvoker())
	const (
		queueARN = "arn:aws:sqs:us-east-1:123456789012:disabled-queue"
		ruleName = "disabled-sched-rule"
	)

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:               ruleName,
		ScheduleExpression: "rate(1 minute)",
		State:              "DISABLED",
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), ruleName, "default", []eventbridge.Target{
		{ID: "t1", Arn: queueARN},
	})
	require.NoError(t, err)

	sched := eventbridge.NewScheduler(b, 0)
	lastFired := map[string]time.Time{}
	sched.ProcessTickForTest(t.Context(), time.Now(), lastFired)

	msgs := sqsMock.MessagesFor(queueARN)
	assert.Empty(t, msgs, "disabled rule must not deliver to targets")
}

func TestAuditEvents_ScheduledRuleNoTargetsDoesNotPanic(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:               "no-target-rule",
		ScheduleExpression: "rate(1 minute)",
		State:              "ENABLED",
	})
	require.NoError(t, err)

	sched := eventbridge.NewScheduler(b, 0)
	lastFired := map[string]time.Time{}
	// Must not panic when no targets are registered.
	require.NotPanics(t, func() {
		sched.ProcessTickForTest(t.Context(), time.Now(), lastFired)
	})
}

// ---------------------------------------------------------------------------
// Pattern matching operators (verify real eval, not stubs)
// ---------------------------------------------------------------------------

func TestAuditEvents_PatternMatching_PrefixSuffixAnythingBut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		pattern       string
		event         eventbridge.EventEntry
		wantDelivered bool
	}{
		{
			name:          "prefix_match_delivers",
			pattern:       `{"source": [{"prefix": "aws."}]}`,
			event:         eventbridge.EventEntry{Source: "aws.s3", DetailType: "ObjectCreated", Detail: `{}`},
			wantDelivered: true,
		},
		{
			name:          "prefix_no_match_skips",
			pattern:       `{"source": [{"prefix": "aws."}]}`,
			event:         eventbridge.EventEntry{Source: "custom.service", DetailType: "Evt", Detail: `{}`},
			wantDelivered: false,
		},
		{
			name:          "suffix_match_delivers",
			pattern:       `{"source": [{"suffix": ".events"}]}`,
			event:         eventbridge.EventEntry{Source: "aws.events", DetailType: "Scheduled Event", Detail: `{}`},
			wantDelivered: true,
		},
		{
			name:          "anything_but_match_delivers",
			pattern:       `{"source": [{"anything-but": ["skip.me"]}]}`,
			event:         eventbridge.EventEntry{Source: "other.src", DetailType: "Evt", Detail: `{}`},
			wantDelivered: true,
		},
		{
			name:          "anything_but_excluded_skips",
			pattern:       `{"source": [{"anything-but": ["skip.me"]}]}`,
			event:         eventbridge.EventEntry{Source: "skip.me", DetailType: "Evt", Detail: `{}`},
			wantDelivered: false,
		},
		{
			name:          "exists_true_delivers_when_field_present",
			pattern:       `{"detail": {"code": [{"exists": true}]}}`,
			event:         eventbridge.EventEntry{Source: "svc", DetailType: "Evt", Detail: `{"code": "200"}`},
			wantDelivered: true,
		},
		{
			name:          "exists_true_skips_when_field_absent",
			pattern:       `{"detail": {"code": [{"exists": true}]}}`,
			event:         eventbridge.EventEntry{Source: "svc", DetailType: "Evt", Detail: `{}`},
			wantDelivered: false,
		},
		{
			name:          "numeric_gt_delivers",
			pattern:       `{"detail": {"count": [{"numeric": [">", 5]}]}}`,
			event:         eventbridge.EventEntry{Source: "svc", DetailType: "Evt", Detail: `{"count": 10}`},
			wantDelivered: true,
		},
		{
			name:          "numeric_gt_skips_below",
			pattern:       `{"detail": {"count": [{"numeric": [">", 5]}]}}`,
			event:         eventbridge.EventEntry{Source: "svc", DetailType: "Evt", Detail: `{"count": 3}`},
			wantDelivered: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sqsMock := newMockSQSSender()
			b := setupDeliveryBackend(t, sqsMock, newMockLambdaInvoker())
			const (
				queueARN = "arn:aws:sqs:us-east-1:123456789012:pattern-queue"
				ruleName = "pattern-rule"
			)

			_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
				Name:         ruleName,
				EventPattern: tc.pattern,
				State:        "ENABLED",
			})
			require.NoError(t, err)

			_, err = b.PutTargets(context.Background(), ruleName, "default", []eventbridge.Target{
				{ID: "t1", Arn: queueARN},
			})
			require.NoError(t, err)

			b.PutEvents(context.Background(), []eventbridge.EventEntry{tc.event})

			// PutEvents delivers asynchronously; give it a moment.
			require.Eventually(t, func() bool {
				msgs := sqsMock.MessagesFor(queueARN)

				return tc.wantDelivered == (len(msgs) > 0)
			}, 2*time.Second, 20*time.Millisecond)
		})
	}
}

// ---------------------------------------------------------------------------
// Custom event bus delivery
// ---------------------------------------------------------------------------

func TestAuditEvents_CustomBus_DeliverToSQS(t *testing.T) {
	t.Parallel()

	sqsMock := newMockSQSSender()
	b := setupDeliveryBackend(t, sqsMock, newMockLambdaInvoker())
	const (
		busName  = "my-custom-bus"
		queueARN = "arn:aws:sqs:us-east-1:123456789012:custom-bus-queue"
		ruleName = "custom-rule"
	)

	_, err := b.CreateEventBus(context.Background(), busName, "")
	require.NoError(t, err)

	_, err = b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         ruleName,
		EventBusName: busName,
		EventPattern: `{"source": ["custom.src"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), ruleName, busName, []eventbridge.Target{
		{ID: "t1", Arn: queueARN},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "custom.src", DetailType: "Evt", Detail: `{"x":1}`, EventBusName: busName},
	})

	require.Eventually(t, func() bool {
		return len(sqsMock.MessagesFor(queueARN)) > 0
	}, 2*time.Second, 20*time.Millisecond, "expected delivery to custom bus SQS target")
}

// ---------------------------------------------------------------------------
// Input transformer delivery
// ---------------------------------------------------------------------------

func TestAuditEvents_InputTransformer_TemplateApplied(t *testing.T) {
	t.Parallel()

	sqsMock := newMockSQSSender()
	b := setupDeliveryBackend(t, sqsMock, newMockLambdaInvoker())
	const (
		queueARN = "arn:aws:sqs:us-east-1:123456789012:transformer-queue"
		ruleName = "transformer-rule"
	)

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         ruleName,
		EventPattern: `{"source": ["svc"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), ruleName, "default", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: queueARN,
			InputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{"env": "$.detail.env"},
				InputTemplate: `{"environment": "<env>"}`,
			},
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "svc", DetailType: "Evt", Detail: `{"env": "production"}`},
	})

	require.Eventually(t, func() bool {
		msgs := sqsMock.MessagesFor(queueARN)

		return len(msgs) > 0 && strings.Contains(msgs[0], "production")
	}, 2*time.Second, 20*time.Millisecond)
}
