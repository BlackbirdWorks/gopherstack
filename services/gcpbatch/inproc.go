package gcpbatch

import (
	"slices"
	"strings"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
)

// API returns the Batch service server for in-process callers.
func (h *Handler) API() batchpb.BatchServiceServer { return &batchServer{b: h.Backend} }

// OperationsAPI returns the long-running operations server for in-process callers.
func (h *Handler) OperationsAPI() longrunningpb.OperationsServer {
	return &operationsServer{b: h.Backend}
}

// ProjectIDs lists the projects that own at least one job, sorted.
func (b *InMemoryBackend) ProjectIDs() []string {
	b.mu.RLock("ProjectIDs")
	defer b.mu.RUnlock()

	seen := map[string]bool{}

	for n := range b.jobs {
		if rest, ok := strings.CutPrefix(n, "projects/"); ok {
			id, _, _ := strings.Cut(rest, "/")
			seen[id] = true
		}
	}

	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}

	slices.Sort(out)

	return out
}
