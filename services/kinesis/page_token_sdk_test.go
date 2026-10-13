package kinesis_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

type inProcessTransport struct{ h http.Handler }

func (tr inProcessTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	tr.h.ServeHTTP(rec, r)

	return rec.Result(), nil
}

// newInProcessKinesisClient drives h through the real SDK without sockets so
// the client can run inside a synctest bubble.
func newInProcessKinesisClient(t *testing.T, h *kinesis.Handler) *kinesissdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return kinesissdk.NewFromConfig(cfg, func(o *kinesissdk.Options) {
		o.BaseEndpoint = aws.String("http://kinesis.local")
		o.HTTPClient = &http.Client{Transport: inProcessTransport{h: e}}
	})
}

func apiErrorCode(t *testing.T, err error) string {
	t.Helper()

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)

	return apiErr.ErrorCode()
}

func TestListShards_NextTokenExpiry_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode string
		wait     time.Duration
	}{
		{name: "fresh", wait: 0},
		{name: "within lifetime", wait: 299 * time.Second},
		{name: "expired", wait: 301 * time.Second, wantCode: "ExpiredNextTokenException"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				client := newInProcessKinesisClient(t, newTestHandler(t))
				_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
					StreamName: aws.String("s"), ShardCount: aws.Int32(3),
				})
				require.NoError(t, err)

				time.Sleep(streamSettleWait)

				page1, err := client.ListShards(t.Context(), &kinesissdk.ListShardsInput{
					StreamName: aws.String("s"), MaxResults: aws.Int32(1),
				})
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(page1.NextToken))
				assert.NotContains(t, aws.ToString(page1.NextToken), "shardId")

				time.Sleep(tc.wait)

				page2, err := client.ListShards(t.Context(), &kinesissdk.ListShardsInput{
					NextToken: page1.NextToken,
				})
				if tc.wantCode != "" {
					require.Error(t, err)
					assert.Equal(t, tc.wantCode, apiErrorCode(t, err))
					var expired *types.ExpiredNextTokenException
					assert.ErrorAs(t, err, &expired)

					return
				}

				require.NoError(t, err)
				require.Len(t, page2.Shards, 2)
				assert.Equal(t, "shardId-000000000001", aws.ToString(page2.Shards[0].ShardId))
			})
		})
	}
}

func TestListStreamConsumers_NextTokenExpiry_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode string
		wait     time.Duration
	}{
		{name: "within lifetime", wait: 299 * time.Second},
		{name: "expired", wait: 301 * time.Second, wantCode: "ExpiredNextTokenException"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				client := newInProcessKinesisClient(t, newTestHandler(t))
				created, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
					StreamName: aws.String("s"), ShardCount: aws.Int32(1),
				})
				require.NoError(t, err)
				require.NotNil(t, created)

				time.Sleep(streamSettleWait)

				desc, err := client.DescribeStreamSummary(t.Context(), &kinesissdk.DescribeStreamSummaryInput{
					StreamName: aws.String("s"),
				})
				require.NoError(t, err)

				streamARN := desc.StreamDescriptionSummary.StreamARN

				for i := range 3 {
					_, err = client.RegisterStreamConsumer(t.Context(), &kinesissdk.RegisterStreamConsumerInput{
						StreamARN: streamARN, ConsumerName: aws.String("c" + strconv.Itoa(i)),
					})
					require.NoError(t, err)
				}

				page1, err := client.ListStreamConsumers(t.Context(), &kinesissdk.ListStreamConsumersInput{
					StreamARN: streamARN, MaxResults: aws.Int32(1),
				})
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(page1.NextToken))
				assert.NotEqual(t, "c0", aws.ToString(page1.NextToken))

				time.Sleep(tc.wait)

				page2, err := client.ListStreamConsumers(t.Context(), &kinesissdk.ListStreamConsumersInput{
					StreamARN: streamARN, NextToken: page1.NextToken,
				})
				if tc.wantCode != "" {
					require.Error(t, err)
					assert.Equal(t, tc.wantCode, apiErrorCode(t, err))

					return
				}

				require.NoError(t, err)
				require.Len(t, page2.Consumers, 2)
				assert.Equal(t, "c1", aws.ToString(page2.Consumers[0].ConsumerName))
			})
		})
	}
}

func TestListShards_TokenParamRules_SDK(t *testing.T) {
	t.Parallel()

	const streamName = "s"

	tests := []struct {
		build    func(token *string, streamARN *string) *kinesissdk.ListShardsInput
		name     string
		wantCode string
	}{
		{
			name: "token alone",
			build: func(token, _ *string) *kinesissdk.ListShardsInput {
				return &kinesissdk.ListShardsInput{NextToken: token}
			},
		},
		{
			name: "token with stream name",
			build: func(token, _ *string) *kinesissdk.ListShardsInput {
				return &kinesissdk.ListShardsInput{NextToken: token, StreamName: aws.String(streamName)}
			},
			wantCode: "InvalidArgumentException",
		},
		{
			name: "token with stream arn",
			build: func(token, arn *string) *kinesissdk.ListShardsInput {
				return &kinesissdk.ListShardsInput{NextToken: token, StreamARN: arn}
			},
			wantCode: "InvalidArgumentException",
		},
		{
			name: "token with creation timestamp",
			build: func(token, _ *string) *kinesissdk.ListShardsInput {
				return &kinesissdk.ListShardsInput{NextToken: token, StreamCreationTimestamp: aws.Time(time.Now())}
			},
			wantCode: "InvalidArgumentException",
		},
		{
			name: "token with exclusive start",
			build: func(token, _ *string) *kinesissdk.ListShardsInput {
				return &kinesissdk.ListShardsInput{
					NextToken:             token,
					ExclusiveStartShardId: aws.String("shardId-000000000000"),
				}
			},
			wantCode: "InvalidArgumentException",
		},
		{
			name: "garbage token",
			build: func(_, _ *string) *kinesissdk.ListShardsInput {
				return &kinesissdk.ListShardsInput{NextToken: aws.String("shardId-000000000000")}
			},
			wantCode: "InvalidArgumentException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestKinesisClient(t, newTestHandler(t))
			_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
				StreamName: aws.String(streamName), ShardCount: aws.Int32(2),
			})
			require.NoError(t, err)

			desc, err := client.DescribeStreamSummary(t.Context(), &kinesissdk.DescribeStreamSummaryInput{
				StreamName: aws.String(streamName),
			})
			require.NoError(t, err)

			page1, err := client.ListShards(t.Context(), &kinesissdk.ListShardsInput{
				StreamName: aws.String(streamName), MaxResults: aws.Int32(1),
			})
			require.NoError(t, err)

			_, err = client.ListShards(t.Context(), tc.build(page1.NextToken, desc.StreamDescriptionSummary.StreamARN))
			if tc.wantCode == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Equal(t, tc.wantCode, apiErrorCode(t, err))
		})
	}
}

func TestListShards_MaxResultsBounds_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantCode   string
		maxResults int32
	}{
		{name: "min", maxResults: 1},
		{name: "max", maxResults: 10000},
		{name: "zero", maxResults: 0, wantCode: "InvalidArgumentException"},
		{name: "negative", maxResults: -1, wantCode: "InvalidArgumentException"},
		{name: "over max", maxResults: 10001, wantCode: "InvalidArgumentException"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestKinesisClient(t, newTestHandler(t))
			_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
				StreamName: aws.String("s"), ShardCount: aws.Int32(1),
			})
			require.NoError(t, err)

			_, err = client.ListShards(t.Context(), &kinesissdk.ListShardsInput{
				StreamName: aws.String("s"), MaxResults: aws.Int32(tc.maxResults),
			})
			if tc.wantCode == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Equal(t, tc.wantCode, apiErrorCode(t, err))
		})
	}
}
