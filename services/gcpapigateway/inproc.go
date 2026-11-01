package gcpapigateway

import (
	"slices"
	"strings"

	"cloud.google.com/go/apigateway/apiv1/apigatewaypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
)

// API returns the control-plane server for in-process callers such as the dashboard proxy.
func (h *Handler) API() apigatewaypb.ApiGatewayServiceServer { return &apiGatewayServer{b: h.Backend} }

// OperationsAPI returns the long-running operations server for in-process callers.
func (h *Handler) OperationsAPI() longrunningpb.OperationsServer {
	return &operationsServer{b: h.Backend}
}

// ProjectIDs lists the projects that own at least one API, config, gateway or operation, sorted.
func (b *InMemoryBackend) ProjectIDs() []string {
	b.mu.RLock("ProjectIDs")
	defer b.mu.RUnlock()

	seen := map[string]bool{}
	add := func(name string) {
		if rest, ok := strings.CutPrefix(name, "projects/"); ok {
			id, _, _ := strings.Cut(rest, "/")
			seen[id] = true
		}
	}

	for n := range b.apis {
		add(n)
	}

	for n := range b.configs {
		add(n)
	}

	for n := range b.gateways {
		add(n)
	}

	for n := range b.ops {
		add(n)
	}

	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	return ids
}
