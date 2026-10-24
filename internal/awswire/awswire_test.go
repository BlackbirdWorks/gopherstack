package awswire_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/awswire"
	"github.com/blackbirdworks/gopherstack/internal/awswire/iam"
	"github.com/blackbirdworks/gopherstack/internal/awswire/kms"
	"github.com/blackbirdworks/gopherstack/internal/awswire/lambda"
	"github.com/blackbirdworks/gopherstack/internal/awswire/sqs"
)

func testConfig(base string) aws.Config {
	cfg := aws.Config{
		Region:      "us-west-2",
		Credentials: credentials.NewStaticCredentialsProvider("AKIDEXAMPLE", "secret", "session-token"),
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
			})
		},
	}
	if base != "" {
		cfg.BaseEndpoint = aws.String(base)
	}

	return cfg
}

func TestClientSignsAndDecodes(t *testing.T) {
	t.Parallel()

	var (
		auth, token, target string
		body                map[string]any
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth, token = r.Header.Get("Authorization"), r.Header.Get("X-Amz-Security-Token")
		target = r.Header.Get("X-Amz-Target")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		_, _ = w.Write([]byte(`{"KeyMetadata":{"KeyId":"k-1","Arn":"arn:aws:kms:us-west-2:1:key/k-1","Enabled":true}}`))
	}))
	t.Cleanup(srv.Close)

	out, err := kms.NewFromConfig(testConfig(srv.URL)).CreateKey(t.Context(), &kms.CreateKeyInput{
		Description: aws.String("d"),
	})
	require.NoError(t, err)

	assert.Equal(t, "k-1", aws.ToString(out.KeyMetadata.KeyId))
	assert.True(t, out.KeyMetadata.Enabled)
	assert.Equal(t, "TrentService.CreateKey", target)
	assert.Equal(t, "session-token", token)
	assert.Contains(t, auth, "Credential=AKIDEXAMPLE/")
	assert.Contains(t, auth, "/us-west-2/kms/aws4_request")
	assert.Equal(t, "d", body["Description"])
	assert.NotContains(t, body, "BypassPolicyLockoutSafetyCheck", "unset bool members are not sent")
}

func TestClientRetries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantCode     string
		throttleCode string
		responses    []int
		wantRequests int32
	}{
		{name: "recovers", responses: []int{500, 503, 200}, wantRequests: 3},
		{name: "gives_up", responses: []int{500, 500, 500, 500}, wantRequests: 3, wantCode: "InternalFailure"},
		{name: "client_error_not_retried", responses: []int{400, 200}, wantRequests: 1, wantCode: "InternalFailure"},
		{name: "throttle", responses: []int{400, 200}, throttleCode: "ThrottlingException", wantRequests: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var n atomic.Int32

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				i := int(n.Add(1)) - 1
				status := tt.responses[min(i, len(tt.responses)-1)]
				w.Header().Set("Content-Type", "application/x-amz-json-1.1")
				w.WriteHeader(status)

				if status == http.StatusOK {
					_, _ = w.Write([]byte(`{"Keys":[],"Truncated":false}`))

					return
				}

				code := "InternalFailure"
				if tt.throttleCode != "" {
					code = tt.throttleCode
				}

				_, _ = w.Write([]byte(`{"__type":"` + code + `","message":"boom"}`))
			}))
			t.Cleanup(srv.Close)

			_, err := kms.NewFromConfig(testConfig(srv.URL)).ListKeys(t.Context(), &kms.ListKeysInput{})
			assert.Equal(t, tt.wantRequests, n.Load())

			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)

			apiErr, ok := errors.AsType[smithy.APIError](err)
			require.True(t, ok)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
		})
	}
}

func TestClientStopsRetryingOnCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())

	var n atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		cancel()
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	_, err := kms.NewFromConfig(testConfig(srv.URL)).ListKeys(ctx, &kms.ListKeysInput{})
	require.Error(t, err)
	assert.Equal(t, int32(1), n.Load())
}

func TestPaginator(t *testing.T) {
	t.Parallel()

	var seen []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)

		var in map[string]any
		_ = json.Unmarshal(raw, &in)

		marker, _ := in["Marker"].(string)
		seen = append(seen, marker)
		assert.NotContains(t, in, "Limit", "the paginator owns the page size")

		w.Header().Set("Content-Type", "application/x-amz-json-1.1")

		switch marker {
		case "":
			_, _ = w.Write([]byte(`{"Keys":[{"KeyId":"a"}],"NextMarker":"m1","Truncated":true}`))
		case "m1":
			_, _ = w.Write([]byte(`{"Keys":[{"KeyId":"b"}],"NextMarker":"m2","Truncated":true}`))
		default:
			_, _ = w.Write([]byte(`{"Keys":[{"KeyId":"c"}],"Truncated":false}`))
		}
	}))
	t.Cleanup(srv.Close)

	p := kms.NewListKeysPaginator(kms.NewFromConfig(testConfig(srv.URL)), &kms.ListKeysInput{Limit: aws.Int32(1)})

	var ids []string

	for p.HasMorePages() {
		page, err := p.NextPage(t.Context())
		require.NoError(t, err)

		for _, k := range page.Keys {
			ids = append(ids, aws.ToString(k.KeyId))
		}
	}

	assert.Equal(t, []string{"a", "b", "c"}, ids)
	assert.Equal(t, []string{"", "m1", "m2"}, seen)

	_, err := p.NextPage(t.Context())
	require.ErrorIs(t, err, awswire.ErrNoMorePages)
}

type recordingTransport struct{ req *http.Request }

func (r *recordingTransport) Do(req *http.Request) (*http.Response, error) {
	r.req = req

	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/xml"}},
		Body: io.NopCloser(strings.NewReader(
			`<ListUsersResponse><ListUsersResult><Users/></ListUsersResult></ListUsersResponse>`)),
		Request: req,
	}, nil
}

func TestClientResolvesRealEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		region      string
		wantHost    string
		wantSigning string
	}{
		{name: "global_service", region: "eu-west-1", wantHost: "iam.amazonaws.com", wantSigning: "/us-east-1/iam/"},
		{
			name: "china", region: "cn-northwest-1", wantHost: "iam.cn-north-1.amazonaws.com.cn",
			wantSigning: "/cn-north-1/iam/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &recordingTransport{}
			cfg := testConfig("")
			cfg.Region, cfg.HTTPClient = tt.region, rt

			_, err := iam.NewFromConfig(cfg).ListUsers(t.Context(), &iam.ListUsersInput{})
			require.NoError(t, err)

			assert.Equal(t, "https", rt.req.URL.Scheme)
			assert.Equal(t, tt.wantHost, rt.req.URL.Host)
			assert.Contains(t, rt.req.Header.Get("Authorization"), tt.wantSigning)
		})
	}
}

type serviceEndpointSource struct{ url string }

func (s serviceEndpointSource) GetServiceBaseEndpoint(_ context.Context, id string) (string, bool, error) {
	return s.url, id == "SQS", nil
}

func TestClientHonoursServiceEndpointConfig(t *testing.T) {
	t.Parallel()

	rt := &recordingTransport{}
	cfg := testConfig("http://ignored.example")
	cfg.HTTPClient = rt
	cfg.ConfigSources = []any{serviceEndpointSource{url: "http://sqs.example:9324"}}

	_, _ = sqs.NewFromConfig(cfg).ListQueues(t.Context(), &sqs.ListQueuesInput{})
	require.NotNil(t, rt.req)
	assert.Equal(t, "sqs.example:9324", rt.req.URL.Host)

	rt.req = nil

	_, _ = kms.NewFromConfig(cfg).ListKeys(t.Context(), &kms.ListKeysInput{})
	require.NotNil(t, rt.req)
	assert.Equal(t, "ignored.example", rt.req.URL.Host)
}

func TestClientOptionsOverrideConfig(t *testing.T) {
	t.Parallel()

	rt := &recordingTransport{}
	cfg := testConfig("http://cfg.example")
	cfg.HTTPClient = rt

	_, _ = kms.NewFromConfig(cfg, func(o *kms.Options) { o.BaseEndpoint = aws.String("http://opt.example") }).
		ListKeys(t.Context(), &kms.ListKeysInput{})
	require.NotNil(t, rt.req)
	assert.Equal(t, "opt.example", rt.req.URL.Host)
}

func TestUnknownServiceAndMissingEndpointFail(t *testing.T) {
	t.Parallel()

	_, err := awswire.New("nope", aws.Config{}, nil).Call(t.Context(), "X", nil)
	require.Error(t, err)

	_, err = awswire.New("route53", aws.Config{Region: "us-east-1"}, nil).Call(t.Context(), "X", nil)
	require.Error(t, err)
}

func TestLambdaInvokeBridgesPayloadAndHeaders(t *testing.T) {
	t.Parallel()

	var path, invType string

	var got []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, invType = r.URL.Path, r.Header.Get("X-Amz-Invocation-Type")
		got, _ = io.ReadAll(r.Body)
		w.Header().Set("X-Amz-Function-Error", "Unhandled")
		w.Header().Set("X-Amz-Executed-Version", "$LATEST")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errorMessage":"boom"}`))
	}))
	t.Cleanup(srv.Close)

	out, err := lambda.NewFromConfig(testConfig(srv.URL)).Invoke(t.Context(), &lambda.InvokeInput{
		FunctionName:   aws.String("fn"),
		InvocationType: lambdatypes.InvocationTypeRequestResponse,
		Payload:        []byte(`{"a":1}`),
	})
	require.NoError(t, err)

	assert.Equal(t, "/2015-03-31/functions/fn/invocations", path)
	assert.Equal(t, "RequestResponse", invType)
	assert.JSONEq(t, `{"a":1}`, string(got))
	assert.Equal(t, int32(200), out.StatusCode)
	assert.Equal(t, "Unhandled", aws.ToString(out.FunctionError))
	assert.JSONEq(t, `{"errorMessage":"boom"}`, string(out.Payload))
}
