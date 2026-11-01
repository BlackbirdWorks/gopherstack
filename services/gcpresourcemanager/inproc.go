package gcpresourcemanager

import (
	"slices"

	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
)

// ProjectsAPI returns the Projects server for in-process callers.
func (h *Handler) ProjectsAPI() resourcemanagerpb.ProjectsServer {
	return projectsServer{b: h.Backend}
}

// ServiceUsageAPI returns the Service Usage server for in-process callers.
func (h *Handler) ServiceUsageAPI() serviceusagepb.ServiceUsageServer {
	return serviceUsageServer{b: h.Backend}
}

// ProjectIDs lists the ids of the projects that are not DELETE_REQUESTED, sorted.
func (b *InMemoryBackend) ProjectIDs() []string {
	b.mu.RLock("ProjectIDs")
	defer b.mu.RUnlock()

	out := make([]string, 0, len(b.projects))

	for _, p := range b.projects {
		if !p.requested() {
			out = append(out, p.ID)
		}
	}

	slices.Sort(out)

	return out
}
