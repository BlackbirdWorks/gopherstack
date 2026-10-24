package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/sdkwire"
)

// Caller invokes one AWS service operation in-process; params may be any JSON-shaped value.
type Caller interface {
	Call(ctx context.Context, op string, params any) (map[string]any, error)
}

type wireCaller struct {
	svc *sdkwire.Service
	tgt sdkwire.Target
}

// NewWireCaller returns a Caller for the sdkwire service key that talks to handler without a generated SDK client.
func NewWireCaller(key string, handler http.Handler, creds aws.CredentialsProvider, region string) (Caller, error) {
	svc, ok := sdkwire.Lookup(key)
	if !ok {
		return nil, fmt.Errorf("%w: %s", sdkwire.ErrUnknownOp, key)
	}

	return &wireCaller{
		svc: svc,
		tgt: sdkwire.Target{Handler: handler, Credentials: creds, Endpoint: "http://local", Region: region},
	}, nil
}

func (c *wireCaller) Call(ctx context.Context, op string, params any) (map[string]any, error) {
	b, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}

	var in map[string]any
	if err = json.Unmarshal(b, &in); err != nil {
		return nil, err
	}

	out, err := c.svc.Call(ctx, c.tgt, op, dropEmpty(in))
	if err != nil {
		return nil, err
	}

	m, _ := out.(map[string]any)

	return m, nil
}

// dropEmpty removes nulls and empty strings so unset struct fields are not sent.
func dropEmpty(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if val == nil || val == "" {
				delete(t, k)

				continue
			}

			t[k] = dropEmpty(val)
		}

		return t
	case []any:
		for i, val := range t {
			t[i] = dropEmpty(val)
		}

		return t
	default:
		return v
	}
}
