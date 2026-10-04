package firehose_test

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	firehosesdk "github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

var errThrottled = errors.New("throttled")

func extendedS3(
	prefix string,
	mutate func(*types.ExtendedS3DestinationConfiguration),
) *types.ExtendedS3DestinationConfiguration {
	cfg := &types.ExtendedS3DestinationConfiguration{
		BucketARN: aws.String("arn:aws:s3:::dest"),
		RoleARN:   aws.String(testRoleARN),
		Prefix:    aws.String(prefix),
	}
	if mutate != nil {
		mutate(cfg)
	}

	return cfg
}

func (e *realismEnv) createS3(t *testing.T, name string, cfg *types.ExtendedS3DestinationConfiguration) {
	t.Helper()

	_, err := e.client.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
		DeliveryStreamName:                 aws.String(name),
		ExtendedS3DestinationConfiguration: cfg,
	})
	require.NoError(t, err)
}

func TestS3Delivery_CompressionFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		format    types.CompressionFormat
		wantExt   string
		wantEncGz bool
	}{
		{format: types.CompressionFormatUncompressed},
		{format: types.CompressionFormatGzip, wantExt: ".gz", wantEncGz: true},
		{format: types.CompressionFormatZip, wantExt: ".zip"},
		{format: types.CompressionFormatSnappy, wantExt: ".snappy"},
		{format: types.CompressionFormatHadoopSnappy, wantExt: ".hsnappy"},
	}

	for _, tt := range tests {
		t.Run(string(tt.format), func(t *testing.T) {
			t.Parallel()

			env := newRealismEnv(t)
			env.createS3(t, "comp", extendedS3("logs/", func(c *types.ExtendedS3DestinationConfiguration) {
				c.CompressionFormat = tt.format
			}))
			env.putAndFlush(t, "comp", "alpha", "beta")

			objs := env.s3.all()
			require.Len(t, objs, 1)
			assert.True(t, strings.HasSuffix(objs[0].key, tt.wantExt), "key %s", objs[0].key)
			assert.Equal(t, tt.wantEncGz, objs[0].encoding == "gzip")
			assert.Equal(t, "alpha\nbeta\n", decodeObject(t, string(tt.format), objs[0].body))
		})
	}
}

func TestS3Delivery_PrefixExpressions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		prefix     string
		wantKeyRe  string
		wantNoTime bool
	}{
		{name: "literal_gets_time_dir", prefix: "data/", wantKeyRe: `^data/\d{4}/\d{2}/\d{2}/\d{2}/s-1-`},
		{
			name:       "timestamp_overrides_default",
			prefix:     "y=!{timestamp:yyyy}/m=!{timestamp:MM}/d=!{timestamp:dd}/",
			wantKeyRe:  `^y=\d{4}/m=\d{2}/d=\d{2}/s-1-`,
			wantNoTime: true,
		},
		{
			name:       "random_string_and_quoted_literal",
			prefix:     "r=!{firehose:random-string}/!{timestamp:'year'yyyy}/",
			wantKeyRe:  `^r=[0-9a-f]{11}/year\d{4}/s-1-`,
			wantNoTime: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newRealismEnv(t)
			env.createS3(t, "s", extendedS3(tt.prefix, func(c *types.ExtendedS3DestinationConfiguration) {
				c.ErrorOutputPrefix = aws.String("err/!{firehose:error-output-type}/")
			}))
			env.putAndFlush(t, "s", "x")

			objs := env.s3.all()
			require.Len(t, objs, 1)
			assert.Regexp(t, tt.wantKeyRe, objs[0].key)

			if tt.wantNoTime {
				assert.NotRegexp(t, `/\d{4}/\d{2}/\d{2}/\d{2}/`, objs[0].key)
			}
		})
	}
}

func dynamicPartitioningStream(query string) func(*types.ExtendedS3DestinationConfiguration) {
	return func(c *types.ExtendedS3DestinationConfiguration) {
		c.DynamicPartitioningConfiguration = &types.DynamicPartitioningConfiguration{Enabled: aws.Bool(true)}
		c.ErrorOutputPrefix = aws.String("errors/!{firehose:error-output-type}/")

		if query != "" {
			c.ProcessingConfiguration = &types.ProcessingConfiguration{
				Enabled: aws.Bool(true),
				Processors: []types.Processor{{
					Type: types.ProcessorTypeMetadataExtraction,
					Parameters: []types.ProcessorParameter{
						{
							ParameterName:  types.ProcessorParameterNameMetadataExtractionQuery,
							ParameterValue: aws.String(query),
						},
						{
							ParameterName:  types.ProcessorParameterNameJsonParsingEngine,
							ParameterValue: aws.String("JQ-1.6"),
						},
					},
				}},
			}
		}
	}
}

func TestS3Delivery_DynamicPartitioning(t *testing.T) {
	t.Parallel()

	t.Run("inline_jq_keys", func(t *testing.T) {
		t.Parallel()

		env := newRealismEnv(t)
		env.createS3(t, "dp", extendedS3("cust=!{partitionKeyFromQuery:cust}/kind=!{partitionKeyFromQuery:kind}/",
			dynamicPartitioningStream(`{cust: .customer.id, kind: .tags[0]}`)))
		env.putAndFlush(t, "dp",
			`{"customer":{"id":"a1"},"tags":["x"]}`,
			`{"customer":{"id":"b2"},"tags":["y"]}`,
			`{"customer":{"id":"a1"},"tags":["x"],"n":2}`,
			`{"customer":{"id":"zz"}}`,
			`not-json`,
		)

		a1 := env.s3.withPrefix("cust=a1/kind=x/")
		require.Len(t, a1, 1)
		assert.Equal(t, 2, strings.Count(string(a1[0].body), "\n"))
		assert.Len(t, env.s3.withPrefix("cust=b2/kind=y/"), 1)

		failed := env.s3.withPrefix("errors/processing-failed/")
		require.Len(t, failed, 1)
		assert.Contains(t, string(failed[0].body), `"zz"`)
		assert.Contains(t, string(failed[0].body), "not-json")
		assert.Equal(t, int64(2), firehose.StreamFailedRecords(env.backend, rtTestRegion, "dp"))
	})

	t.Run("lambda_metadata_keys", func(t *testing.T) {
		t.Parallel()

		env := newRealismEnv(t)
		env.backend.SetLambdaBackend(&fakeLambda{respond: contentTransform})
		env.createS3(
			t,
			"dpl",
			extendedS3("tenant=!{partitionKeyFromLambda:tenant}/", func(c *types.ExtendedS3DestinationConfiguration) {
				dynamicPartitioningStream("")(c)
				c.ProcessingConfiguration = lambdaProcessing()
			}),
		)
		env.putAndFlush(t, "dpl", "acme-1", "globex-1", "acme-2")

		acme := env.s3.withPrefix("tenant=acme/")
		require.Len(t, acme, 1)
		assert.Equal(t, "ACME-1\nACME-2\n", string(acme[0].body))
		assert.Len(t, env.s3.withPrefix("tenant=globex/"), 1)
	})
}

func TestS3Delivery_LambdaProcessorResults(t *testing.T) {
	t.Parallel()

	env := newRealismEnv(t)
	fn := &fakeLambda{respond: contentTransform}
	env.backend.SetLambdaBackend(fn)
	env.createS3(t, "proc", extendedS3("out/", func(c *types.ExtendedS3DestinationConfiguration) {
		c.ProcessingConfiguration = lambdaProcessing()
		c.ErrorOutputPrefix = aws.String("bad/!{firehose:error-output-type}/")
	}))
	env.putAndFlush(t, "proc", "ok-one", "drop-two", "fail-three", "ok-four")

	var ev transformEvent
	require.Equal(t, 1, fn.callCount())
	require.NoError(t, json.Unmarshal(fn.payloads[0], &ev))
	assert.NotEmpty(t, ev.InvocationID)
	assert.Equal(t, rtTestRegion, ev.Region)
	assert.Regexp(t, `^arn:aws:firehose:us-east-1:000000000000:deliverystream/proc$`, ev.DeliveryStreamARN)
	require.Len(t, ev.Records, 4)
	assert.Positive(t, ev.Records[0].ApproximateArrivalTimestamp)

	out := env.s3.withPrefix("out/")
	require.Len(t, out, 1)
	assert.Equal(t, "OK-ONE\nOK-FOUR\n", string(out[0].body))

	failed := env.s3.withPrefix("bad/processing-failed/")
	require.Len(t, failed, 1)

	var envlp map[string]string
	require.NoError(t, json.Unmarshal(failed[0].body, &envlp))
	assert.Equal(t, testLambdaARN, envlp["lambdaARN"])
	assert.Equal(t, "ProcessingFailed", envlp["errorCode"])
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("fail-three")), envlp["rawData"])
	assert.Equal(t, int64(1), firehose.StreamFailedRecords(env.backend, rtTestRegion, "proc"))
}

func TestS3Delivery_LambdaContractViolations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		respond   func([]byte, int) ([]byte, error)
		wantCode  string
		wantCalls int
	}{
		{
			name:      "too_few_records",
			respond:   func([]byte, int) ([]byte, error) { return []byte(`{"records":[]}`), nil },
			wantCode:  "Lambda.JsonMappingException",
			wantCalls: 1,
		},
		{
			name: "unknown_record_id",
			respond: func([]byte, int) ([]byte, error) {
				return []byte(`{"records":[{"recordId":"nope","result":"Ok","data":"eA=="}]}`), nil
			},
			wantCode:  "Lambda.MissingRecordId",
			wantCalls: 1,
		},
		{
			name: "duplicate_record_id",
			respond: func([]byte, int) ([]byte, error) {
				return []byte(
					`{"records":[{"recordId":"0","result":"Ok","data":"eA=="},{"recordId":"0","result":"Ok","data":"eA=="}]}`,
				), nil
			},
			wantCode:  "Lambda.DuplicatedRecordId",
			wantCalls: 1,
		},
		{
			name:      "function_error_payload",
			respond:   func([]byte, int) ([]byte, error) { return []byte(`{"errorMessage":"boom"}`), nil },
			wantCode:  "Lambda.FunctionError",
			wantCalls: 1,
		},
		{
			name:      "invocation_retried_three_times",
			respond:   func([]byte, int) ([]byte, error) { return nil, errThrottled },
			wantCode:  "Lambda.InvocationFailure",
			wantCalls: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newRealismEnv(t)
			fn := &fakeLambda{respond: tt.respond}
			env.backend.SetLambdaBackend(fn)
			env.createS3(t, "viol", extendedS3("out/", func(c *types.ExtendedS3DestinationConfiguration) {
				c.ProcessingConfiguration = lambdaProcessing()
				c.ErrorOutputPrefix = aws.String("bad/!{firehose:error-output-type}/")
			}))
			env.putAndFlush(t, "viol", "one", "two")

			assert.Equal(t, tt.wantCalls, fn.callCount())
			assert.Empty(t, env.s3.withPrefix("out/"))

			failed := env.s3.withPrefix("bad/processing-failed/")
			require.Len(t, failed, 1)
			assert.Equal(t, 2, strings.Count(string(failed[0].body), tt.wantCode))
			assert.Equal(t, int64(2), firehose.StreamFailedRecords(env.backend, rtTestRegion, "viol"))
		})
	}
}

func TestDelivery_RoleAuthorization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		role     string
		deny     string
		wantCode string
	}{
		{
			name:     "lambda_policy",
			role:     testRoleARN,
			deny:     "lambda:InvokeFunction",
			wantCode: "Lambda.InvokeAccessDenied",
		},
		{
			name:     "lambda_trust",
			role:     "arn:aws:iam::000000000000:role/untrusted",
			wantCode: "Lambda.AssumeRoleAccessDenied",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newRealismEnv(t)
			fn := &fakeLambda{respond: contentTransform}
			env.backend.SetLambdaBackend(fn)
			env.backend.SetRoleAuthorizer(denyAuthorizer{deny: map[string]bool{tt.deny: true, "s3:PutObject": false}})
			env.createS3(t, "auth", extendedS3("out/", func(c *types.ExtendedS3DestinationConfiguration) {
				c.ProcessingConfiguration = lambdaProcessing()
				c.RoleARN = aws.String(tt.role)
				c.ErrorOutputPrefix = aws.String("bad/!{firehose:error-output-type}/")
			}))
			env.putAndFlush(t, "auth", "one")

			if tt.name == "lambda_trust" {
				assert.Empty(t, env.s3.all(), "an unassumable role is denied S3 too, so nothing lands")
				assert.Zero(t, fn.callCount())

				return
			}

			assert.Zero(t, fn.callCount(), "denied role must not invoke the function")
			failed := env.s3.withPrefix("bad/processing-failed/")
			require.Len(t, failed, 1)
			assert.Contains(t, string(failed[0].body), tt.wantCode)
		})
	}
}

func httpDestination(
	url string,
	mutate func(*types.HttpEndpointDestinationConfiguration),
) *types.HttpEndpointDestinationConfiguration {
	cfg := &types.HttpEndpointDestinationConfiguration{
		EndpointConfiguration: &types.HttpEndpointConfiguration{Url: aws.String(url), AccessKey: aws.String("sekret")},
		S3Configuration: &types.S3DestinationConfiguration{
			BucketARN: aws.String("arn:aws:s3:::backup"), RoleARN: aws.String(testRoleARN),
			ErrorOutputPrefix: aws.String("http-err/!{firehose:error-output-type}/"),
		},
		RetryOptions: &types.HttpEndpointRetryOptions{DurationInSeconds: aws.Int32(1)},
		RoleARN:      aws.String(testRoleARN),
	}
	if mutate != nil {
		mutate(cfg)
	}

	return cfg
}

type endpointCall struct {
	headers http.Header
	body    []byte
}

// endpoint replies with the next scripted behaviour for each request.
type endpoint struct {
	script func(call int, w http.ResponseWriter, r *http.Request, requestID string)
	calls  []endpointCall
	mu     sync.Mutex
}

func (e *endpoint) serve(t *testing.T) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw []byte

		if r.Header.Get("Content-Encoding") == "gzip" {
			raw = gunzipReader(t, r)
		} else {
			raw = readAll(r)
		}

		e.mu.Lock()
		e.calls = append(e.calls, endpointCall{headers: r.Header.Clone(), body: raw})
		n := len(e.calls)
		e.mu.Unlock()

		e.script(n, w, r, r.Header.Get("X-Amz-Firehose-Request-Id"))
	}))
	t.Cleanup(srv.Close)

	return srv
}

func replyOK(w http.ResponseWriter, requestID string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"requestId": requestID, "timestamp": time.Now().UnixMilli()})
}

func TestHTTPEndpoint_RequestContract(t *testing.T) {
	t.Parallel()

	ep := &endpoint{script: func(_ int, w http.ResponseWriter, _ *http.Request, id string) { replyOK(w, id) }}
	srv := ep.serve(t)

	env := newRealismEnv(t)
	_, err := env.client.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("h"),
		HttpEndpointDestinationConfiguration: httpDestination(
			srv.URL,
			func(c *types.HttpEndpointDestinationConfiguration) {
				c.RequestConfiguration = &types.HttpEndpointRequestConfiguration{
					ContentEncoding: types.ContentEncodingGzip,
					CommonAttributes: []types.HttpEndpointCommonAttribute{
						{AttributeName: aws.String("env"), AttributeValue: aws.String("prod")},
					},
				}
			},
		),
	})
	require.NoError(t, err)
	env.putAndFlush(t, "h", "hello", "world")

	require.Len(t, ep.calls, 1)
	call := ep.calls[0]
	assert.Equal(t, "1.0", call.headers.Get("X-Amz-Firehose-Protocol-Version"))
	assert.Equal(t, "application/json", call.headers.Get("Content-Type"))
	assert.Equal(t, "sekret", call.headers.Get("X-Amz-Firehose-Access-Key"))
	assert.Equal(
		t,
		"arn:aws:firehose:us-east-1:000000000000:deliverystream/h",
		call.headers.Get("X-Amz-Firehose-Source-Arn"),
	)
	assert.JSONEq(t, `{"commonAttributes":{"env":"prod"}}`, call.headers.Get("X-Amz-Firehose-Common-Attributes"))

	var body struct {
		RequestID string `json:"requestId"`
		Records   []struct {
			Data string `json:"data"`
		} `json:"records"`
		Timestamp int64 `json:"timestamp"`
	}
	require.NoError(t, json.Unmarshal(call.body, &body))
	assert.Equal(t, call.headers.Get("X-Amz-Firehose-Request-Id"), body.RequestID)
	assert.Positive(t, body.Timestamp)
	require.Len(t, body.Records, 2)
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("world")), body.Records[1].Data)
	assert.Empty(t, env.s3.all())
}

func TestHTTPEndpoint_FailureHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		script      func(call int, w http.ResponseWriter, r *http.Request, id string)
		name        string
		wantCode    string
		minCalls    int
		wantBackup  bool
		wantSameIDs bool
	}{
		{
			name: "retry_then_success_keeps_request_id",
			script: func(call int, w http.ResponseWriter, _ *http.Request, id string) {
				if call == 1 {
					http.Error(
						w,
						`{"requestId":"`+id+`","timestamp":1,"errorMessage":"later"}`,
						http.StatusServiceUnavailable,
					)

					return
				}

				replyOK(w, id)
			},
			minCalls:    2,
			wantSameIDs: true,
		},
		{
			name: "persistent_5xx_goes_to_backup",
			script: func(_ int, w http.ResponseWriter, _ *http.Request, _ string) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"errorMessage":"down"}`))
			},
			wantBackup: true, wantCode: "HttpEndpoint.DestinationException", minCalls: 2,
		},
		{
			name: "bare_200_violates_contract",
			script: func(_ int, w http.ResponseWriter, _ *http.Request, _ string) {
				w.WriteHeader(http.StatusOK)
			},
			wantBackup: true, wantCode: "HttpEndpoint.InvalidResponseFromDestination", minCalls: 2,
		},
		{
			name: "wrong_request_id_violates_contract",
			script: func(_ int, w http.ResponseWriter, _ *http.Request, _ string) {
				replyOK(w, "other")
			},
			wantBackup: true, wantCode: "HttpEndpoint.InvalidResponseFromDestination", minCalls: 2,
		},
		{
			name: "413_is_permanent_and_not_backed_up",
			script: func(_ int, w http.ResponseWriter, _ *http.Request, _ string) {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
			},
			minCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ep := &endpoint{script: tt.script}
			srv := ep.serve(t)

			env := newRealismEnv(t)
			_, err := env.client.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
				DeliveryStreamName:                   aws.String("hf"),
				HttpEndpointDestinationConfiguration: httpDestination(srv.URL, nil),
			})
			require.NoError(t, err)
			env.putAndFlush(t, "hf", "payload")

			ep.mu.Lock()
			calls := append([]endpointCall{}, ep.calls...)
			ep.mu.Unlock()

			require.GreaterOrEqual(t, len(calls), tt.minCalls)

			if tt.wantSameIDs {
				assert.Equal(
					t,
					calls[0].headers.Get("X-Amz-Firehose-Request-Id"),
					calls[1].headers.Get("X-Amz-Firehose-Request-Id"),
				)
			}

			backup := env.s3.withPrefix("http-err/http-endpoint-failed/")
			if !tt.wantBackup {
				assert.Empty(t, backup)

				return
			}

			require.Len(t, backup, 1)
			assert.Contains(t, string(backup[0].body), tt.wantCode)
			assert.Contains(t, string(backup[0].body), base64.StdEncoding.EncodeToString([]byte("payload")))
			assert.Equal(t, int64(1), firehose.StreamFailedRecords(env.backend, rtTestRegion, "hf"))
		})
	}
}

func TestOpenSearch_InProcessDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rotation types.AmazonopensearchserviceIndexRotationPeriod
		wantRe   string
	}{
		{name: "no_rotation", rotation: types.AmazonopensearchserviceIndexRotationPeriodNoRotation, wantRe: `^events$`},
		{
			name:     "one_day",
			rotation: types.AmazonopensearchserviceIndexRotationPeriodOneDay,
			wantRe:   `^events-\d{4}-\d{2}-\d{2}$`,
		},
		{
			name: "one_month", wantRe: `^events-\d{4}-\d{2}$`,
			rotation: types.AmazonopensearchserviceIndexRotationPeriodOneMonth,
		},
		{
			name: "one_week", wantRe: `^events-\d{4}-w\d{2}$`,
			rotation: types.AmazonopensearchserviceIndexRotationPeriodOneWeek,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newRealismEnv(t)
			idx := &fakeIndexer{}
			env.backend.SetOpenSearchBackend(idx)

			_, err := env.client.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
				DeliveryStreamName: aws.String("os"),
				AmazonopensearchserviceDestinationConfiguration: &types.AmazonopensearchserviceDestinationConfiguration{
					DomainARN:           aws.String("arn:aws:es:us-east-1:000000000000:domain/logs"),
					IndexName:           aws.String("events"),
					IndexRotationPeriod: tt.rotation,
					RoleARN:             aws.String(testRoleARN),
					S3Configuration: &types.S3DestinationConfiguration{
						BucketARN: aws.String("arn:aws:s3:::backup"), RoleARN: aws.String(testRoleARN),
						ErrorOutputPrefix: aws.String("os-err/!{firehose:error-output-type}/"),
					},
				},
			})
			require.NoError(t, err)
			env.putAndFlush(t, "os", `{"level":"info","n":1}`, `plain text`)

			docs := idx.all()
			require.Len(t, docs, 1)
			assert.Equal(t, "logs", docs[0].domain)
			assert.Regexp(t, tt.wantRe, docs[0].index)
			assert.Equal(t, "info", docs[0].doc["level"])

			failed := env.s3.withPrefix("os-err/AmazonOpenSearchService-failed/")
			require.Len(t, failed, 1)
			assert.Contains(t, string(failed[0].body), base64.StdEncoding.EncodeToString([]byte("plain text")))
		})
	}
}

func TestOpenSearch_RoleDenied(t *testing.T) {
	t.Parallel()

	env := newRealismEnv(t)
	idx := &fakeIndexer{}
	env.backend.SetOpenSearchBackend(idx)
	env.backend.SetRoleAuthorizer(denyAuthorizer{deny: map[string]bool{"es:ESHttpPost": true}})

	_, err := env.client.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("osd"),
		AmazonopensearchserviceDestinationConfiguration: &types.AmazonopensearchserviceDestinationConfiguration{
			DomainARN: aws.String("arn:aws:es:us-east-1:000000000000:domain/logs"),
			IndexName: aws.String("events"),
			RoleARN:   aws.String(testRoleARN),
			S3Configuration: &types.S3DestinationConfiguration{
				BucketARN: aws.String("arn:aws:s3:::backup"), RoleARN: aws.String(testRoleARN),
				ErrorOutputPrefix: aws.String("os-err/!{firehose:error-output-type}/"),
			},
		},
	})
	require.NoError(t, err)
	env.putAndFlush(t, "osd", `{"a":1}`)

	assert.Empty(t, idx.all())

	failed := env.s3.withPrefix("os-err/AmazonOpenSearchService-failed/")
	require.Len(t, failed, 1)
	assert.Contains(t, string(failed[0].body), "ES.AccessDenied")
}

func TestKinesisSource_DeliversToS3(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := newFirehoseBackend(t)
		s3 := &s3Recorder{}
		b.SetS3Backend(s3)

		kin := &mockKinesisReader{}
		kin.addRecords([]byte("k1"), []byte("k2"))
		b.SetKinesisBackend(kin)

		_, err := b.CreateDeliveryStream(t.Context(), firehose.CreateDeliveryStreamInput{
			Name:               "ks",
			DeliveryStreamType: "KinesisStreamAsSource",
			Source: &firehose.SourceDescription{
				KinesisStreamSourceDescription: &firehose.KinesisStreamSourceDescription{
					KinesisStreamARN: "arn:aws:kinesis:us-east-1:123456789012:stream/src",
				},
			},
			S3Destination: &firehose.S3DestinationDescription{
				BucketARN:         "arn:aws:s3:::dest",
				CompressionFormat: "GZIP",
			},
		})
		require.NoError(t, err)

		synctest.Wait()
		b.FlushAll(t.Context())

		objs := s3.all()
		require.Len(t, objs, 1)
		assert.Equal(t, "k1\nk2\n", decodeObject(t, "GZIP", objs[0].body))
	})
}
