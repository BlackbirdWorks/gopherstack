package main

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventBridge_DefaultBusExistsInEveryRegion(t *testing.T) {
	t.Parallel()

	for _, region := range []string{regionA, regionB, "ap-southeast-2"} {
		t.Run(region, func(t *testing.T) {
			t.Parallel()

			out, err := eventbridge.NewFromConfig(regionConfig(t, region)).DescribeEventBus(t.Context(),
				&eventbridge.DescribeEventBusInput{})
			require.NoError(t, err)
			assert.Equal(t, "default", aws.ToString(out.Name))
			assert.Contains(t, aws.ToString(out.Arn), ":"+region+":")
		})
	}
}

func TestS3Notifications_NonHomeBucketUsesRegionalEventBridgeAndPayloadRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "home-region", region: regionA},
		{name: "other-region", region: regionB},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			cfg := regionConfig(t, tc.region)
			sqsC := sqs.NewFromConfig(cfg)
			ebC := eventbridge.NewFromConfig(cfg)
			s3C := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
			suffix := strings.ReplaceAll(tc.region, "-", "")

			ebQueue := newRegionQueue(t, sqsC, "eb-"+suffix)
			s3Queue := newRegionQueue(t, sqsC, "s3q-"+suffix)

			_, err := ebC.PutRule(ctx, &eventbridge.PutRuleInput{
				Name:         aws.String("s3-" + suffix),
				EventPattern: aws.String(`{"source":["aws.s3"]}`),
			})
			require.NoError(t, err)

			_, err = ebC.PutTargets(ctx, &eventbridge.PutTargetsInput{
				Rule:    aws.String("s3-" + suffix),
				Targets: []ebtypes.Target{{Id: aws.String("t"), Arn: aws.String(ebQueue.arn)}},
			})
			require.NoError(t, err)

			bucket := "notif-" + suffix
			create := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
			if tc.region != regionA {
				create.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
					LocationConstraint: s3types.BucketLocationConstraint(tc.region),
				}
			}

			_, err = s3C.CreateBucket(ctx, create)
			require.NoError(t, err)

			_, err = s3C.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
				Bucket: aws.String(bucket),
				NotificationConfiguration: &s3types.NotificationConfiguration{
					EventBridgeConfiguration: &s3types.EventBridgeConfiguration{},
					QueueConfigurations: []s3types.QueueConfiguration{{
						QueueArn: aws.String(s3Queue.arn),
						Events:   []s3types.Event{s3types.EventS3ObjectCreatedPut},
					}},
				},
			})
			require.NoError(t, err)

			_, err = s3C.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket), Key: aws.String("k"), Body: strings.NewReader("x"),
			})
			require.NoError(t, err)

			assert.Contains(t, s3Queue.receive(t, sqsC), `"awsRegion":"`+tc.region+`"`)
			assert.Contains(t, ebQueue.receive(t, sqsC), `"region":"`+tc.region+`"`)
		})
	}
}

type regionQueue struct {
	url string
	arn string
}

func newRegionQueue(t *testing.T, c *sqs.Client, name string) regionQueue {
	t.Helper()

	created, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String(name)})
	require.NoError(t, err)

	attrs, err := c.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl: created.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)

	arn := attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]

	return regionQueue{url: aws.ToString(created.QueueUrl), arn: arn}
}

func (q regionQueue) receive(t *testing.T, c *sqs.Client) string {
	t.Helper()

	var body string

	require.Eventually(t, func() bool {
		out, err := c.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{QueueUrl: aws.String(q.url)})
		if err != nil || len(out.Messages) == 0 {
			return false
		}

		body = aws.ToString(out.Messages[0].Body)

		return true
	}, 10*time.Second, 50*time.Millisecond)

	return body
}
