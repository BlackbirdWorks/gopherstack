// Package awswire is a small AWS SDK-shaped client over services/stepfunctions/sdkwire: typed requests go out
// through the Smithy protocol codecs with SigV4 signing and the SDK's standard retries, so callers need no
// generated service client. The per-service packages under this directory are written by cmd/ccwiregen.
package awswire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions/sdkwire"
)

const defaultTimeout = 5 * time.Minute

// ErrNoEndpoint is returned when a service has no endpoint rules here and no base endpoint is configured.
var ErrNoEndpoint = errors.New("no endpoint configured")

// Options are the per-client settings a caller may override after the config is applied.
type Options struct {
	BaseEndpoint *string
	Credentials  aws.CredentialsProvider
	HTTPClient   aws.HTTPClient
	Retryer      func() aws.Retryer
	Region       string
}

// Core sends the operations of one service.
type Core struct {
	retryer func() aws.Retryer
	err     error
	svc     *sdkwire.Service
	tgt     sdkwire.Target
}

// New builds the Core for sdkwire service key from cfg; a bad key surfaces as the error of every call.
func New(key string, cfg aws.Config, optFns []func(*Options)) *Core {
	o := Options{Credentials: cfg.Credentials, HTTPClient: cfg.HTTPClient, Retryer: cfg.Retryer, Region: cfg.Region}

	for _, fn := range optFns {
		fn(&o)
	}

	svc, ok := sdkwire.Lookup(key)
	if !ok {
		return &Core{err: fmt.Errorf("%w: %s", sdkwire.ErrUnknownOp, key)}
	}

	cfg.Region = o.Region

	ep := resolve(key, cfg, o.BaseEndpoint)
	if info := services[key]; info.baseOnly && ep.url == "" {
		return &Core{err: fmt.Errorf("%w for %s", ErrNoEndpoint, key)}
	}

	hc := o.HTTPClient
	if hc == nil {
		hc = httpclient.New(defaultTimeout)
	}

	retryer := o.Retryer
	if retryer == nil {
		retryer = func() aws.Retryer { return retry.NewStandard() }
	}

	return &Core{
		svc: svc, retryer: retryer,
		tgt: sdkwire.Target{
			Handler: forwarder{c: hc}, Credentials: o.Credentials, Endpoint: ep.url, Region: ep.signingRegion,
		},
	}
}

// Call sends params as op, retrying like the SDK's standard retryer, and returns the JSON-shaped response.
func (c *Core) Call(ctx context.Context, op string, params map[string]any) (any, error) {
	if c.err != nil {
		return nil, c.err
	}

	r := c.retryer()
	release := r.GetInitialToken()

	for attempt := 1; ; attempt++ {
		out, err := c.svc.Call(ctx, c.tgt, op, params)
		if err == nil {
			_ = release(nil)

			return out, nil
		}

		if ctx.Err() != nil || attempt >= r.MaxAttempts() || !r.IsErrorRetryable(err) {
			_ = release(err)

			return nil, err
		}

		next, tokenErr := r.GetRetryToken(ctx, err)
		if tokenErr != nil {
			return nil, err
		}

		delay, delayErr := r.RetryDelay(attempt, err)
		if delayErr != nil {
			return nil, err
		}

		if !sleep(ctx, delay) {
			return nil, err
		}

		release = next
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Do sends in (a JSON-shaped struct or map) as op and decodes the response into O.
func Do[O any](ctx context.Context, c *Core, op string, in any) (*O, error) {
	params := map[string]any{}

	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return nil, fmt.Errorf("encode %s request: %w", op, err)
		}

		if err = json.Unmarshal(b, &params); err != nil {
			return nil, fmt.Errorf("encode %s request: %w", op, err)
		}
	}

	raw, err := c.Call(ctx, op, params)
	if err != nil {
		return nil, err
	}

	var out O

	b, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}

	if err = json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("decode %s response: %w", op, err)
	}

	return &out, nil
}

// forwarder serves a request built by sdkwire by sending it through the configured HTTP client.
type forwarder struct{ c aws.HTTPClient }

func (f forwarder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	out := r.Clone(r.Context())
	out.RequestURI = ""

	resp, err := f.c.Do(out)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)

		return
	}

	defer resp.Body.Close()

	maps.Copy(w.Header(), resp.Header)

	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
