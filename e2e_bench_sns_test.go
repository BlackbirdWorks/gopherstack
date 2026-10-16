package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdksns "github.com/aws/aws-sdk-go-v2/service/sns"
	sdksqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
)

const (
	e2eSNSQueues  = 3
	e2eSNSHTTPSub = 3
)

func BenchmarkE2E_SNS(b *testing.B) {
	e, _ := benchServer(b)
	cfg := newBenchAWSConfig(b, benchTransport{e: e})
	ctx := context.Background()

	snsC := sdksns.NewFromConfig(cfg, func(o *sdksns.Options) { o.BaseEndpoint = aws.String(benchEndpoint) })
	sqsC := sdksqs.NewFromConfig(cfg, func(o *sdksqs.Options) { o.BaseEndpoint = aws.String(benchEndpoint) })

	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	b.Cleanup(sink.Close)

	newTopic := func(b *testing.B, name string, queues, httpSubs int) string {
		b.Helper()

		tp, err := snsC.CreateTopic(ctx, &sdksns.CreateTopicInput{Name: aws.String(name)})
		require.NoError(b, err)

		for i := range queues {
			q, qerr := sqsC.CreateQueue(ctx, &sdksqs.CreateQueueInput{
				QueueName: aws.String(name + "-q" + strconv.Itoa(i)),
			})
			require.NoError(b, qerr)

			attrs, aerr := sqsC.GetQueueAttributes(ctx, &sdksqs.GetQueueAttributesInput{
				QueueUrl:       q.QueueUrl,
				AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
			})
			require.NoError(b, aerr)

			_, serr := snsC.Subscribe(ctx, &sdksns.SubscribeInput{
				TopicArn: tp.TopicArn, Protocol: aws.String("sqs"),
				Endpoint: aws.String(attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]),
			})
			require.NoError(b, serr)
		}

		for range httpSubs {
			_, serr := snsC.Subscribe(ctx, &sdksns.SubscribeInput{
				TopicArn: tp.TopicArn, Protocol: aws.String("http"), Endpoint: aws.String(sink.URL),
			})
			require.NoError(b, serr)
		}

		return aws.ToString(tp.TopicArn)
	}

	for _, tc := range []struct {
		name          string
		queues, https int
	}{
		{"publish_no_subs", 0, 0},
		{"publish_3sqs", e2eSNSQueues, 0},
		{"publish_3sqs_3http", e2eSNSQueues, e2eSNSHTTPSub},
	} {
		b.Run(tc.name, func(b *testing.B) {
			arn := newTopic(b, "e2e-bench-"+tc.name, tc.queues, tc.https)

			b.ReportAllocs()

			for b.Loop() {
				_, err := snsC.Publish(ctx, &sdksns.PublishInput{
					TopicArn: aws.String(arn), Message: aws.String("e2e bench notification"),
					Subject: aws.String("bench"),
				})
				require.NoError(b, err)
			}
		})
	}
}
