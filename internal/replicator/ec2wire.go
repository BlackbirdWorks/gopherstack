package replicator

import (
	"context"
	"io"
	"maps"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/sdkwire"
)

// ec2Wire calls EC2 over the ec2Query wire without linking the generated EC2 SDK client.
type ec2Wire struct {
	svc *sdkwire.Service
	tgt sdkwire.Target
}

func newEC2Wire(cfg aws.Config, hc *http.Client) *ec2Wire {
	svc, _ := sdkwire.Lookup("ec2")

	return &ec2Wire{
		svc: svc,
		tgt: sdkwire.Target{
			Handler: httpForwarder{c: hc}, Credentials: cfg.Credentials,
			Endpoint: ec2Endpoint(cfg), Region: cfg.Region,
		},
	}
}

// idsWhere returns the key member of each item of listKey in a Describe call filtered by filterName in values.
func (w *ec2Wire) idsWhere(
	ctx context.Context,
	op, listKey, key, filterName string,
	values []string,
) ([]string, error) {
	vs := make([]any, 0, len(values))
	for _, v := range values {
		vs = append(vs, v)
	}

	out, err := w.svc.Call(ctx, w.tgt, op, map[string]any{
		"Filters": []any{map[string]any{"Name": filterName, "Values": vs}},
	})
	if err != nil {
		return nil, err
	}

	m, _ := out.(map[string]any)
	items, _ := m[listKey].([]any)
	ids := make([]string, 0, len(items))

	for _, it := range items {
		o, _ := it.(map[string]any)
		id, _ := o[key].(string)
		ids = append(ids, id)
	}

	return ids, nil
}

// httpForwarder serves an in-process request by forwarding it over the network.
type httpForwarder struct{ c *http.Client }

func (f httpForwarder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	out := r.Clone(r.Context())
	out.RequestURI = ""

	resp, err := f.c.Do(out) //nolint:gosec // the request was built by sdkwire for the configured endpoint
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)

		return
	}

	defer resp.Body.Close()

	maps.Copy(w.Header(), resp.Header)

	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
