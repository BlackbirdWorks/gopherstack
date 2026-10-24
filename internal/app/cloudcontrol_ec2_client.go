package app

import (
	"context"
	"fmt"
	"maps"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/sdkwire"
)

// ccEC2Client calls the in-process EC2 handler over the ec2Query wire without the generated SDK client.
type ccEC2Client struct {
	svc *sdkwire.Service
	tgt sdkwire.Target
}

func newCCEC2Client(handler http.Handler, cfg aws.Config) (*ccEC2Client, error) {
	svc, ok := sdkwire.Lookup("ec2")
	if !ok {
		return nil, fmt.Errorf("%w: ec2", sdkwire.ErrUnknownOp)
	}

	return &ccEC2Client{
		svc: svc,
		tgt: sdkwire.Target{
			Handler:     handler,
			Credentials: cfg.Credentials,
			Endpoint:    aws.ToString(cfg.BaseEndpoint),
			Region:      cfg.Region,
		},
	}, nil
}

func (c *ccEC2Client) call(ctx context.Context, op string, params map[string]any) (map[string]any, error) {
	out, err := c.svc.Call(ctx, c.tgt, op, params)
	if err != nil {
		return nil, err
	}

	m, _ := out.(map[string]any)

	return m, nil
}

// describe pages through a Describe operation and returns the items under listKey.
func (c *ccEC2Client) describe(
	ctx context.Context,
	op, listKey string,
	params map[string]any,
) ([]map[string]any, error) {
	var items []map[string]any

	p := maps.Clone(params)
	if p == nil {
		p = map[string]any{}
	}

	for {
		out, err := c.call(ctx, op, p)
		if err != nil {
			return nil, err
		}

		items = append(items, ccMapList(out, listKey)...)

		next := ccMapStr(out, "NextToken")
		if next == "" {
			return items, nil
		}

		p["NextToken"] = next
	}
}

func ccMapStr(m map[string]any, key string) string {
	s, _ := m[key].(string)

	return s
}

func ccMapBool(m map[string]any, key string) bool {
	b, _ := m[key].(bool)

	return b
}

func ccMapObj(m map[string]any, key string) map[string]any {
	o, _ := m[key].(map[string]any)

	return o
}

func ccMapList(m map[string]any, key string) []map[string]any {
	raw, _ := m[key].([]any)
	out := make([]map[string]any, 0, len(raw))

	for _, it := range raw {
		if o, ok := it.(map[string]any); ok {
			out = append(out, o)
		}
	}

	return out
}

func ccEC2Filter(name string, values ...string) map[string]any {
	vs := make([]any, 0, len(values))
	for _, v := range values {
		vs = append(vs, v)
	}

	return map[string]any{ccKeyName: name, "Values": vs}
}

func ccSetStr(m map[string]any, key, value string) {
	if value != "" {
		m[key] = value
	}
}
