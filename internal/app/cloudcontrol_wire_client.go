package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"

	cloudcontrolbackend "github.com/blackbirdworks/gopherstack/services/cloudcontrol"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions/sdkwire"
)

// ccWireClient calls an in-process service handler over its wire protocol without the generated SDK client.
type ccWireClient struct {
	svc *sdkwire.Service
	tgt sdkwire.Target
}

func newCCWireClient(key string, handler http.Handler, cfg aws.Config) (*ccWireClient, error) {
	svc, ok := sdkwire.Lookup(key)
	if !ok {
		return nil, fmt.Errorf("%w: %s", sdkwire.ErrUnknownOp, key)
	}

	return &ccWireClient{
		svc: svc,
		tgt: sdkwire.Target{
			Handler:     handler,
			Credentials: cfg.Credentials,
			Endpoint:    aws.ToString(cfg.BaseEndpoint),
			Region:      cfg.Region,
		},
	}, nil
}

// wireCall sends in (any JSON-shaped struct or map) as op and decodes the response into O.
func wireCall[O any](ctx context.Context, c *ccWireClient, op string, in any) (*O, error) {
	params := map[string]any{}

	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", cloudcontrolbackend.ErrValidation, err)
		}

		if err = json.Unmarshal(b, &params); err != nil {
			return nil, fmt.Errorf("%w: %w", cloudcontrolbackend.ErrValidation, err)
		}
	}

	raw, err := c.svc.Call(ctx, c.tgt, op, params)
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
