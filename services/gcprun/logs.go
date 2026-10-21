package gcprun

import (
	"net/http"

	"cloud.google.com/go/run/apiv2/runpb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

// SetLogSink wires Cloud Logging: container output and ingress requests are written to sink; nil disables it.
func (b *InMemoryBackend) SetLogSink(sink gcpserve.LogSink) { b.logs.SetSink(sink) }

func revisionLogTarget(rev *runpb.Revision, instance string) gcpserve.LogTarget {
	n, err := parseName(rev.GetName(), "services", "revisions")
	if err != nil {
		return gcpserve.LogTarget{}
	}

	t := gcpserve.LogTarget{
		Project:  n.project,
		Resource: gcpserve.CloudRunResource(n.project, n.ids[0], n.ids[1], n.location),
	}

	if instance != "" {
		t.Labels = map[string]string{"instanceId": instance}
	}

	return t
}

func (b *InMemoryBackend) captureOutput(runner Runner, id string, rev *runpb.Revision) {
	if t := revisionLogTarget(rev, id); t.Resource != nil {
		b.logs.Start(b.baseCtx(), runner, id, t)
	}
}

// serveLogged proxies the request to port and writes its request log entry.
func (b *InMemoryBackend) serveLogged(w http.ResponseWriter, r *http.Request, port int, t gcpserve.LogTarget) {
	if t.Resource == nil {
		b.proxy(port).ServeHTTP(w, r)

		return
	}

	rec := gcpserve.NewRequestRecorder(w)
	start := b.now()

	b.proxy(port).ServeHTTP(rec, r)
	b.logs.Request(b.baseCtx(), t, r, rec, b.now().Sub(start))
}

func (b *InMemoryBackend) ingressTarget(revName string) (int, gcpserve.LogTarget) {
	b.mu.RLock("ingressTarget")
	defer b.mu.RUnlock()

	rev := b.revisions[revName]
	if rev == nil {
		return 0, gcpserve.LogTarget{}
	}

	return rev.hostPort, revisionLogTarget(rev.pb, rev.containerID)
}
