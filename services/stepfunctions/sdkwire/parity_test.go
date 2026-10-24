package sdkwire

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	"github.com/aws/smithy-go/traits"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testEndpoint = "http://sfn-sdk-integration.local"

// capturingClient records the request and answers with a canned response.
type capturingClient struct {
	respHeader http.Header
	req        *http.Request
	reqBody    []byte
	respBody   []byte
	respStatus int
}

func (c *capturingClient) Do(r *http.Request) (*http.Response, error) {
	c.req = r

	c.reqBody = nil
	if r.Body != nil {
		c.reqBody, _ = io.ReadAll(r.Body)
	}

	status := c.respStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}

	body := c.respBody
	if body == nil {
		body = []byte("{}")
	}

	h := c.respHeader
	if h == nil {
		h = http.Header{}
	}

	return &http.Response{
		StatusCode: status, Body: io.NopCloser(bytes.NewReader(body)), Header: h.Clone(),
		Request: r, ContentLength: int64(len(body)),
	}, nil
}

func newSDKClient(build func(aws.Config) any, hc aws.HTTPClient) any {
	return build(aws.Config{
		Region:               "us-east-1",
		HTTPClient:           hc,
		BaseEndpoint:         aws.String(testEndpoint),
		Credentials:          credentials.NewStaticCredentialsProvider("test", "test", ""),
		Retryer:              func() aws.Retryer { return aws.NopRetryer{} },
		AuthSchemePreference: []string{"sigv4"},
		APIOptions: []func(*middleware.Stack) error{func(s *middleware.Stack) error {
			_, _ = s.Initialize.Remove("OperationInputValidation")

			return nil
		}},
	})
}

type sdkOp struct {
	method reflect.Value
	in     reflect.Type
	out    reflect.Type
	name   string
}

func sdkOps(client any) []sdkOp {
	v := reflect.ValueOf(client)
	t := v.Type()

	var ops []sdkOp

	for i := range t.NumMethod() {
		m := t.Method(i)
		if m.Type.NumIn() != 4 || !m.Type.IsVariadic() || m.Type.NumOut() != 2 ||
			m.Type.In(2).Kind() != reflect.Pointer || strings.Contains(m.Name, "Bidirectional") {
			continue
		}

		ops = append(ops, sdkOp{name: m.Name, method: v.Method(i), in: m.Type.In(2), out: m.Type.Out(0)})
	}

	return ops
}

func callSDK(op sdkOp, in reflect.Value) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	res := op.method.Call([]reflect.Value{reflect.ValueOf(context.Background()), in})
	err, _ = res[1].Interface().(error)

	return err
}

var ignoredHeaders = map[string]bool{
	"Authorization": true, "X-Amz-Date": true, "User-Agent": true, "Amz-Sdk-Invocation-Id": true,
	"Amz-Sdk-Request": true, "X-Amz-Security-Token": true, "X-Amz-Content-Sha256": true, "Accept-Encoding": true,
	"X-Amz-User-Agent": true, "Content-Length": true, "X-Amz-Account-Id": true, "Host": true,
	"X-Amz-Sdk-Checksum-Algorithm": true, "Content-Md5": true, "Trailer": true, "Content-Encoding": true,
	"X-Amz-Decoded-Content-Length": true, "Expect": true, "X-Amz-Region-Set": true,
}

func headerDiff(want, got http.Header) []string {
	var diffs []string

	keys := map[string]bool{}
	for k := range want {
		keys[k] = true
	}

	for k := range got {
		keys[k] = true
	}

	for k := range keys {
		if ignoredHeaders[k] || strings.HasPrefix(k, "X-Amz-Checksum-") {
			continue
		}

		if !slices.Equal(want.Values(k), got.Values(k)) {
			diffs = append(diffs, fmt.Sprintf("header %s: sdk=%v ours=%v", k, want.Values(k), got.Values(k)))
		}
	}

	return diffs
}

func bodyDiff(contentType string, want, got []byte) string {
	if bytes.Equal(want, got) {
		return ""
	}

	if strings.Contains(contentType, "json") {
		var w, g any

		if len(bytes.TrimSpace(want)) == 0 {
			w = map[string]any{}
		} else if json.Unmarshal(want, &w) != nil {
			return fmt.Sprintf("body: sdk not json: %s", want)
		}

		if len(bytes.TrimSpace(got)) == 0 {
			g = map[string]any{}
		} else if json.Unmarshal(got, &g) != nil {
			return fmt.Sprintf("body: ours not json: %s", got)
		}

		if w, g = dropDefaults(w), dropDefaults(g); reflect.DeepEqual(w, g) {
			return ""
		}

		return "body " + jsonDiff("", w, g)
	}

	return fmt.Sprintf("body: sdk=%.300q ours=%.300q", want, got)
}

func requestDiff(sdkReq *http.Request, sdkBody []byte, ours *http.Request, ourBody []byte) []string {
	var diffs []string

	if sdkReq.Method != ours.Method {
		diffs = append(diffs, fmt.Sprintf("method: sdk=%s ours=%s", sdkReq.Method, ours.Method))
	}

	if sdkReq.URL.EscapedPath() != ours.URL.EscapedPath() {
		diffs = append(diffs, fmt.Sprintf("path: sdk=%s ours=%s", sdkReq.URL.EscapedPath(), ours.URL.EscapedPath()))
	}

	if sq, oq := sdkReq.URL.Query(), ours.URL.Query(); !reflect.DeepEqual(sq, oq) {
		diffs = append(diffs, fmt.Sprintf("query: sdk=%v ours=%v", sq, oq))
	}

	diffs = append(diffs, headerDiff(sdkReq.Header, ours.Header)...)

	if d := bodyDiff(sdkReq.Header.Get("Content-Type"), sdkBody, ourBody); d != "" {
		diffs = append(diffs, d)
	}

	return diffs
}

// sqsChecksummed operations fail the SDK's client-side MD5 check on sentinel data.
var sqsChecksummed = map[string]bool{"ReceiveMessage": true, "SendMessage": true, "SendMessageBatch": true}

// compressedRequests are gzipped by the SDK; the emulator reads plain bodies.
var compressedRequests = map[string]bool{"PutMetricData": true}

func serviceKeys() []string {
	keys := Keys()
	sort.Strings(keys)

	return keys
}

func TestRequestParity(t *testing.T) {
	t.Parallel()

	builders := sdkClients()

	for _, key := range serviceKeys() {
		t.Run(key, func(t *testing.T) {
			t.Parallel()

			svc, ok := Lookup(key)
			require.True(t, ok)

			cc := &capturingClient{}
			client := newSDKClient(builders[key], cc)
			ops := sdkOps(client)
			require.NotEmpty(t, ops)

			var failures []string

			for _, op := range ops {
				if d := requestParityOp(svc, cc, op); len(d) > 0 {
					failures = append(failures, op.name+": "+strings.Join(d, "; "))
				}
			}

			assert.Empty(t, failures, "%d of %d operations differ from the SDK", len(failures), len(ops))
			assert.LessOrEqual(t, len(svc.Ops), len(ops)+1)
		})
	}
}

func requestParityOp(svc *Service, cc *capturingClient, op sdkOp) []string {
	opName := op.name

	o, ok := svc.Ops[opName]
	if !ok {
		return []string{"operation missing from registry"}
	}

	if o.Schema.IsInputEventStream() || o.Schema.IsOutputEventStream() || compressedRequests[opName] {
		return nil
	}

	in := fill(op.in, 0, map[reflect.Type]int{})
	params, _ := toValue(in)

	cc.req = nil
	_ = callSDK(op, in)

	if cc.req == nil {
		return []string{"sdk produced no request"}
	}

	tgt := Target{
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
		Endpoint:    testEndpoint, Region: "us-east-1",
	}

	req, err := svc.buildRequest(context.Background(), tgt, opName, o, params)
	if err != nil {
		return []string{"build: " + err.Error()}
	}

	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
	}

	return requestDiff(cc.req, cc.reqBody, req, body)
}

// dropDefaults removes zero scalars: the SDK always writes non-nullable
// members of a present struct, the emulator reads them the same as absent.
func dropDefaults(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))

		for k, e := range t {
			if isDefaultScalar(e) {
				continue
			}

			out[k] = dropDefaults(e)
		}

		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = dropDefaults(e)
		}

		return out
	default:
		return v
	}
}

func isDefaultScalar(v any) bool {
	switch t := v.(type) {
	case float64:
		return t == 0
	case bool:
		return !t
	default:
		return false
	}
}

// jsonDiff names the first path at which two decoded JSON values differ.
func jsonDiff(path string, want, got any) string {
	wm, wok := want.(map[string]any)
	gm, gok := got.(map[string]any)

	if wok && gok {
		keys := map[string]bool{}
		for k := range wm {
			keys[k] = true
		}

		for k := range gm {
			keys[k] = true
		}

		sorted := make([]string, 0, len(keys))
		for k := range keys {
			sorted = append(sorted, k)
		}

		sort.Strings(sorted)

		for _, k := range sorted {
			if !reflect.DeepEqual(wm[k], gm[k]) {
				return jsonDiff(path+"."+k, wm[k], gm[k])
			}
		}
	}

	wl, wok := want.([]any)
	gl, gok := got.([]any)

	if wok && gok && len(wl) == len(gl) {
		for i := range wl {
			if !reflect.DeepEqual(wl[i], gl[i]) {
				return jsonDiff(fmt.Sprintf("%s[%d]", path, i), wl[i], gl[i])
			}
		}
	}

	return fmt.Sprintf("%s: sdk=%.80v ours=%.80v", path, want, got)
}

// wireResponse renders value as the response to op by serializing it with the
// service protocol as if it were a request body and bindings.
func wireResponse(svc *Service, o *Op, value any) (http.Header, []byte, error) {
	synthetic := smithy.NewOperationSchema(
		smithy.NewSchema(smithy.ShapeID{Namespace: "test", Name: "Response"}, smithy.ShapeTypeOperation, 0,
			&traits.HTTP{Method: http.MethodPost, URI: "/"}),
		o.Schema.Output, nil,
	)

	sreq, _ := smithyhttp.NewStackRequest().(*smithyhttp.Request)
	sreq.URL, _ = url.Parse(testEndpoint)

	in := &input{value: value, schema: o.Schema.Output}

	ctx := middleware.WithOperationName(context.Background(), "Response")
	if err := svc.Protocol.SerializeRequest(ctx, synthetic, in, sreq); err != nil {
		return nil, nil, err
	}

	if in.err != nil {
		return nil, nil, in.err
	}

	req := sreq.Build(ctx)

	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
	}

	return req.Header, body, nil
}

func TestResponseParity(t *testing.T) {
	t.Parallel()

	builders := sdkClients()

	for _, key := range serviceKeys() {
		t.Run(key, func(t *testing.T) {
			t.Parallel()

			svc, ok := Lookup(key)
			require.True(t, ok)

			var failures []string

			for _, op := range sdkOps(newSDKClient(builders[key], &capturingClient{})) {
				if d := responseParityOp(svc, builders[key], op); d != "" {
					failures = append(failures, op.name+": "+d)
				}
			}

			assert.Empty(t, failures, "%d operations decode differently from the SDK", len(failures))
		})
	}
}

func responseParityOp(svc *Service, build func(aws.Config) any, op sdkOp) string {
	o, ok := svc.Ops[op.name]
	if !ok {
		return "operation missing from registry"
	}

	if o.Schema.IsInputEventStream() || o.Schema.IsOutputEventStream() || o.Schema.Output == nil ||
		sqsChecksummed[op.name] {
		return ""
	}

	outValue := fill(op.out.Elem(), 0, map[reflect.Type]int{})
	want, _ := toValue(outValue)

	header, body, err := wireResponse(svc, o, want)
	if err != nil {
		return "render: " + err.Error()
	}

	header.Del("Content-Length")

	cc := &capturingClient{respStatus: http.StatusOK, respHeader: header, respBody: body}
	client := newSDKClient(build, cc)
	in := fill(op.in, 0, map[reflect.Type]int{})
	params, _ := toValue(in)

	res := reflect.ValueOf(client).MethodByName(op.name).Call([]reflect.Value{reflect.ValueOf(context.Background()), in})
	if err, _ := res[1].Interface().(error); err != nil {
		return "sdk: " + err.Error()
	}

	sdkValue, _ := toValue(res[0])

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for k, v := range header {
			w.Header()[k] = v
		}

		_, _ = w.Write(body)
	})

	tgt := Target{
		Handler: handler, Endpoint: testEndpoint, Region: "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	}

	got, err := svc.Call(context.Background(), tgt, op.name, params)
	if err != nil {
		return "ours: " + err.Error()
	}

	if reflect.DeepEqual(sdkValue, got) {
		return ""
	}

	return jsonDiff("", sdkValue, got)
}
