package elbv2_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testFnARN = "arn:aws:lambda:us-east-1:123456789012:function:web"

type fakeInvoker struct {
	err      error
	arn      string
	response string
	fnErr    string
	payload  []byte
	mu       sync.Mutex
}

func (f *fakeInvoker) InvokeFunction(_ context.Context, arn string, payload []byte) ([]byte, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.arn, f.payload = arn, payload

	return []byte(f.response), f.fnErr, f.err
}

func (f *fakeInvoker) event(t *testing.T) map[string]any {
	t.Helper()

	f.mu.Lock()
	defer f.mu.Unlock()

	var ev map[string]any

	require.NoError(t, json.Unmarshal(f.payload, &ev))

	return ev
}

func lambdaFixture(t *testing.T, multiValue bool, inv *fakeInvoker) (*dpFixture, string) {
	t.Helper()

	f := newDPFixture(t)
	f.h.SetLambdaInvoker(inv)

	tg := f.tg(t, "tg-fn", elbv2types.TargetTypeEnumLambda, "")
	f.register(t, tg, testFnARN)

	if multiValue {
		_, err := f.client.ModifyTargetGroupAttributes(t.Context(), &elbv2sdk.ModifyTargetGroupAttributesInput{
			TargetGroupArn: aws.String(tg),
			Attributes: []elbv2types.TargetGroupAttribute{
				{Key: aws.String("lambda.multi_value_headers.enabled"), Value: aws.String("true")},
			},
		})
		require.NoError(t, err)
	}

	f.listener(t, forwardAction(tg))

	return f, tg
}

func TestDataPlane_LambdaEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantQuery  string
		wantHdrKey string
		multiValue bool
	}{
		{name: "single value", wantQuery: "queryStringParameters", wantHdrKey: "headers"},
		{
			name: "multi value", multiValue: true, wantQuery: "multiValueQueryStringParameters",
			wantHdrKey: "multiValueHeaders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inv := &fakeInvoker{response: `{"statusCode":200,"body":"ok"}`}
			f, tg := lambdaFixture(t, tt.multiValue, inv)

			got := f.do(t, dpReq{
				method: http.MethodPost, path: "/items/1?a=1&a=2&b=x%20y",
				headers: map[string]string{"Content-Type": "application/json", "X-Custom": "v"}, body: `{"k":1}`,
			})
			require.Equal(t, http.StatusOK, got.status)

			ev := inv.event(t)
			assert.Equal(t, testFnARN, inv.arn)
			assert.Equal(t, "POST", ev["httpMethod"])
			assert.Equal(t, "/items/1", ev["path"])
			assert.Equal(t, `{"k":1}`, ev["body"])
			assert.Equal(t, false, ev["isBase64Encoded"])
			assert.Equal(t, tg, ev["requestContext"].(map[string]any)["elb"].(map[string]any)["targetGroupArn"])
			require.Contains(t, ev, tt.wantQuery)
			require.Contains(t, ev, tt.wantHdrKey)

			hdrs := ev[tt.wantHdrKey].(map[string]any)
			assert.Contains(t, hdrs, "x-custom")
			assert.Contains(t, hdrs, "x-forwarded-for")
			assert.Contains(t, hdrs, "x-amzn-trace-id")

			q := ev[tt.wantQuery].(map[string]any)
			if tt.multiValue {
				assert.Equal(t, []any{"1", "2"}, q["a"])
				assert.NotContains(t, ev, "queryStringParameters")
			} else {
				assert.Equal(t, "2", q["a"])
				assert.Equal(t, "x%20y", q["b"])
				assert.NotContains(t, ev, "multiValueHeaders")
			}
		})
	}
}

func TestDataPlane_LambdaBinaryBody(t *testing.T) {
	t.Parallel()

	inv := &fakeInvoker{response: `{"statusCode":200}`}
	f, _ := lambdaFixture(t, false, inv)

	f.do(t, dpReq{
		method:  http.MethodPut,
		path:    "/",
		headers: map[string]string{"Content-Type": "application/octet-stream"},
		body:    "\xff\x00bin",
	})

	ev := inv.event(t)
	assert.Equal(t, true, ev["isBase64Encoded"])
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("\xff\x00bin")), ev["body"])
}

func TestDataPlane_LambdaResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantHeader map[string][]string
		name       string
		response   string
		fnErr      string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "plain",
			response:   `{"statusCode":201,"statusDescription":"201 Created","headers":{"X-A":"1"},"body":"made"}`,
			wantStatus: http.StatusCreated, wantBody: "made", wantHeader: map[string][]string{"X-A": {"1"}},
		},
		{
			name:       "multi value headers",
			response:   `{"statusCode":200,"multiValueHeaders":{"Set-Cookie":["a=1","b=2"]},"body":""}`,
			wantStatus: http.StatusOK, wantHeader: map[string][]string{"Set-Cookie": {"a=1", "b=2"}},
		},
		{
			name:       "base64",
			response:   `{"statusCode":200,"isBase64Encoded":true,"body":"AAEC"}`,
			wantStatus: http.StatusOK, wantBody: "\x00\x01\x02",
		},
		{name: "missing status", response: `{"body":"x"}`, wantStatus: http.StatusBadGateway},
		{name: "not json", response: `nope`, wantStatus: http.StatusBadGateway},
		{name: "function error", response: `{"statusCode":200}`, fnErr: "Unhandled", wantStatus: http.StatusBadGateway},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f, _ := lambdaFixture(t, false, &fakeInvoker{response: tt.response, fnErr: tt.fnErr})

			got := f.do(t, dpReq{path: "/"})
			require.Equal(t, tt.wantStatus, got.status)

			if tt.wantStatus == http.StatusBadGateway {
				return
			}

			assert.Equal(t, tt.wantBody, got.body)

			for k, v := range tt.wantHeader {
				assert.Equal(t, v, got.header.Values(k))
			}
		})
	}
}

func TestDataPlane_LambdaWithoutInvoker(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	tg := f.tg(t, "tg-fn", elbv2types.TargetTypeEnumLambda, "")
	f.register(t, tg, testFnARN)
	f.listener(t, forwardAction(tg))

	assert.Equal(t, http.StatusServiceUnavailable, f.do(t, dpReq{path: "/"}).status)
}

type fakeResolver map[string]string

func (r fakeResolver) InstanceAddress(id string) (string, bool) {
	ip, ok := r[id]

	return ip, ok
}

func TestDataPlane_InstanceTarget(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	be := newBackend(t, "inst", http.StatusOK)
	f.h.SetInstanceAddressResolver(fakeResolver{"i-0123456789abcdef0": "127.0.0.1"})

	tg := f.tg(t, "tg-i", elbv2types.TargetTypeEnumInstance, "/health")
	f.register(t, tg, "i-0123456789abcdef0:"+portOf(be.addr()))
	f.listener(t, forwardAction(tg))

	got := f.do(t, dpReq{path: "/q"})
	require.Equal(t, http.StatusOK, got.status)
	assert.Equal(t, "inst:/q", got.body)
}

func portOf(addr string) string {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[i+1:]
		}
	}

	return addr
}
