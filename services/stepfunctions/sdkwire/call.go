package sdkwire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/google/uuid"
)

const (
	headerAccountID = "X-Amz-Account-Id"
	userAgent       = "aws-sdk-go-v2/gopherstack-sfn "
)

var (
	// ErrUnknownOp is returned for an operation the service does not define.
	ErrUnknownOp = errors.New("unknown AWS SDK action")
	// ErrEventStream is returned for operations that stream events.
	ErrEventStream = errors.New("event stream operations are not supported")
)

// Target is where and as whom a call is made.
type Target struct {
	Handler     http.Handler
	Credentials aws.CredentialsProvider
	Endpoint    string
	Region      string
	// Account is sent as X-Amz-Account-Id; empty for the default account.
	Account string
}

// Call invokes operation op (e.g. "SendMessage") with Step Functions style
// params and returns the response as a JSON-shaped value. Service errors come
// back as *awshttp.ResponseError wrapping a smithy.APIError.
func (s *Service) Call(ctx context.Context, tgt Target, op string, params any) (any, error) {
	o, ok := s.Ops[op]
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownOp, op)
	}

	if o.Schema.IsInputEventStream() || o.Schema.IsOutputEventStream() {
		return nil, fmt.Errorf("%w: %s", ErrEventStream, op)
	}

	req, err := s.buildRequest(ctx, tgt, op, o, withToken(o, params))
	if err != nil {
		return nil, err
	}

	rec := httptest.NewRecorder()
	tgt.Handler.ServeHTTP(rec, req)

	res := rec.Result()
	res.Request = req

	resp := &smithyhttp.Response{Response: res}
	out := &output{schema: o.Schema.Output}

	if err := s.Protocol.DeserializeResponse(ctx, o.Schema, &smithy.TypeRegistry{}, resp, out); err != nil {
		return nil, &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{Response: resp, Err: err},
			RequestID:     requestID(res.Header),
		}
	}

	if out.value == nil {
		return map[string]any{}, nil
	}

	return out.value, nil
}

func (s *Service) buildRequest(ctx context.Context, tgt Target, op string, o *Op, params any) (*http.Request, error) {
	base, err := url.Parse(tgt.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("endpoint: %w", err)
	}

	sreq, _ := smithyhttp.NewStackRequest().(*smithyhttp.Request)
	sreq.URL = base

	in := &input{value: params, schema: o.Schema.Input}
	if o.Schema.Input != nil && params == nil {
		in.value = map[string]any{}
	}

	if err := s.Protocol.SerializeRequest(middleware.WithOperationName(ctx, op), o.Schema, in, sreq); err != nil {
		return nil, fmt.Errorf("serialize request: %w", err)
	}

	if in.err != nil {
		return nil, in.err
	}

	req := sreq.Build(ctx)
	req.Header.Set("User-Agent", userAgent+s.UserAgentAPI)

	for k, v := range s.Headers {
		req.Header.Set(k, v)
	}

	if tgt.Account != "" {
		req.Header.Set(headerAccountID, tgt.Account)
	}

	return req, s.sign(ctx, tgt, req)
}

func (s *Service) sign(ctx context.Context, tgt Target, req *http.Request) error {
	creds, err := tgt.Credentials.Retrieve(ctx)
	if err != nil {
		return fmt.Errorf("retrieve credentials: %w", err)
	}

	body := []byte{}

	if req.Body != nil {
		if body, err = io.ReadAll(req.Body); err != nil {
			return fmt.Errorf("read request body: %w", err)
		}

		req.Body = io.NopCloser(bytes.NewReader(body))
	}

	sum := sha256.Sum256(body)

	signingName := s.SigningName
	if signingName == "" {
		signingName = strings.TrimPrefix(strings.SplitN(s.UserAgentAPI, "#", 2)[0], "api/")
	}

	return v4.NewSigner().SignHTTP(ctx, creds, req, hex.EncodeToString(sum[:]), signingName, tgt.Region, time.Now())
}

func requestID(h http.Header) string {
	for _, name := range []string{"X-Amzn-Requestid", "X-Amz-Requestid"} {
		if v := h.Get(name); v != "" {
			return v
		}
	}

	return ""
}

// withToken copies params with the operation's idempotency token filled in.
func withToken(o *Op, params any) any {
	if o.Token == "" {
		return params
	}

	m, ok := params.(map[string]any)
	if !ok && params != nil {
		return params
	}

	if v, found := lookupKey(m, o.Token); found && v != nil {
		return params
	}

	out := make(map[string]any, len(m)+1)
	for k, v := range m {
		out[k] = v
	}

	out[o.Token] = uuid.NewString()

	return out
}
