package chaos_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

const injectedCode = "ChaosInjectedError"

func chaosServer(t *testing.T, store *chaos.FaultStore, hits *atomic.Int64) *httptest.Server {
	t.Helper()

	e := echo.New()
	e.Use(chaos.Middleware(store))
	e.Any("/*", func(c *echo.Context) error {
		if hits != nil {
			hits.Add(1)
		}

		return c.NoContent(http.StatusOK)
	})

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

func sdkConfig() aws.Config {
	return aws.Config{
		Region:           "us-east-1",
		Credentials:      credentials.NewStaticCredentialsProvider("AKID", "SECRET", ""),
		RetryMaxAttempts: 1,
	}
}

func TestMiddleware_InjectedErrorParsedBySDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call    func(ctx context.Context, cfg aws.Config, url string) error
		name    string
		service string
	}{
		{
			name: "awsjson10_dynamodb", service: "dynamodb",
			call: func(ctx context.Context, cfg aws.Config, url string) error {
				_, err := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = &url }).
					ListTables(ctx, &dynamodb.ListTablesInput{})

				return err
			},
		},
		{
			name: "awsjson10_sqs", service: "sqs",
			call: func(ctx context.Context, cfg aws.Config, url string) error {
				_, err := sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = &url }).
					ListQueues(ctx, &sqs.ListQueuesInput{})

				return err
			},
		},
		{
			name: "awsjson11_kms", service: "kms",
			call: func(ctx context.Context, cfg aws.Config, url string) error {
				_, err := kms.NewFromConfig(cfg, func(o *kms.Options) { o.BaseEndpoint = &url }).
					ListKeys(ctx, &kms.ListKeysInput{})

				return err
			},
		},
		{
			name: "restjson_lambda", service: "lambda",
			call: func(ctx context.Context, cfg aws.Config, url string) error {
				_, err := lambda.NewFromConfig(cfg, func(o *lambda.Options) { o.BaseEndpoint = &url }).
					ListFunctions(ctx, &lambda.ListFunctionsInput{})

				return err
			},
		},
		{
			name: "query_sns", service: "sns",
			call: func(ctx context.Context, cfg aws.Config, url string) error {
				_, err := sns.NewFromConfig(cfg, func(o *sns.Options) { o.BaseEndpoint = &url }).
					ListTopics(ctx, &sns.ListTopicsInput{})

				return err
			},
		},
		{
			name: "query_iam", service: "iam",
			call: func(ctx context.Context, cfg aws.Config, url string) error {
				_, err := iam.NewFromConfig(cfg, func(o *iam.Options) { o.BaseEndpoint = &url }).
					ListUsers(ctx, &iam.ListUsersInput{})

				return err
			},
		},
		{
			name: "ec2query_ec2", service: "ec2",
			call: func(ctx context.Context, cfg aws.Config, url string) error {
				_, err := ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.BaseEndpoint = &url }).
					DescribeInstances(ctx, &ec2.DescribeInstancesInput{})

				return err
			},
		},
		{
			name: "restxml_s3", service: "s3",
			call: func(ctx context.Context, cfg aws.Config, url string) error {
				_, err := s3.NewFromConfig(cfg, func(o *s3.Options) {
					o.BaseEndpoint = &url
					o.UsePathStyle = true
				}).ListBuckets(ctx, &s3.ListBucketsInput{})

				return err
			},
		},
		{
			name: "restxml_cloudfront", service: "cloudfront",
			call: func(ctx context.Context, cfg aws.Config, url string) error {
				_, err := cloudfront.NewFromConfig(cfg, func(o *cloudfront.Options) { o.BaseEndpoint = &url }).
					ListDistributions(ctx, &cloudfront.ListDistributionsInput{})

				return err
			},
		},
		{
			name: "restxml_route53", service: "route53",
			call: func(ctx context.Context, cfg aws.Config, url string) error {
				_, err := route53.NewFromConfig(cfg, func(o *route53.Options) { o.BaseEndpoint = &url }).
					ListHostedZones(ctx, &route53.ListHostedZonesInput{})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := chaos.NewFaultStore()
			store.SetRules([]chaos.FaultRule{{
				Service: tt.service,
				Error:   &chaos.FaultError{Code: injectedCode, StatusCode: http.StatusBadRequest},
			}})

			srv := chaosServer(t, store, nil)

			err := tt.call(t.Context(), sdkConfig(), srv.URL)
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, injectedCode, apiErr.ErrorCode())
		})
	}
}

func TestMiddleware_ProbabilitySemantics(t *testing.T) {
	t.Parallel()

	const requests = 200

	tests := []struct {
		prob     *float64
		name     string
		wantHits int64
	}{
		{name: "omitted_always_fires", prob: nil, wantHits: 0},
		{name: "explicit_zero_never_fires", prob: new(0.0), wantHits: requests},
		{name: "explicit_one_always_fires", prob: new(1.0), wantHits: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := chaos.NewFaultStore()
			store.SetRules([]chaos.FaultRule{{Service: "dynamodb", Probability: tt.prob}})

			var hits atomic.Int64

			srv := chaosServer(t, store, &hits)

			for range requests {
				req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL, nil)
				require.NoError(t, err)
				req.Header.Set("Authorization", buildSigV4Auth("dynamodb", "us-east-1"))
				req.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")

				resp, err := testclient.New(t).Do(req)
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
			}

			assert.Equal(t, tt.wantHits, hits.Load())
		})
	}
}

func TestFaultRule_ProbabilityJSONRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "omitted_stays_omitted", body: `[{"service":"s3"}]`},
		{name: "zero_preserved", body: `[{"service":"s3","probability":0}]`},
		{name: "fraction_preserved", body: `[{"service":"s3","probability":0.25}]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, _ := lsServer(t)
			rec := lsCall(e, http.MethodPost, "/_localstack/chaos/faults", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.JSONEq(t, tt.body, lsCall(e, http.MethodGet, "/_localstack/chaos/faults", "").Body.String())
		})
	}
}
