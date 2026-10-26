package gcpcloudbuild

import (
	"strings"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
)

// API exposes the Cloud Build service plus stored build logs for in-process callers (dashboard proxy).
type API struct {
	cloudbuildpb.CloudBuildServer
	b *InMemoryBackend
}

// API returns the in-process Cloud Build API.
func (h *Handler) API() *API { return &API{CloudBuildServer: &server{b: h.Backend}, b: h.Backend} }

// BuildLog returns the build's log from its logs bucket; ok is false when it has not been stored (yet).
func (a *API) BuildLog(build *cloudbuildpb.Build) ([]byte, bool) {
	a.b.mu.RLock("BuildLog")
	store := a.b.svc.Store
	a.b.mu.RUnlock()

	bucket, _, _ := strings.Cut(strings.TrimPrefix(build.GetLogsBucket(), "gs://"), "/")
	if store == nil || bucket == "" {
		return nil, false
	}

	data, err := store.ReadObject(bucket, "log-"+build.GetId()+".txt")
	if err != nil {
		return nil, false
	}

	return data, true
}
