package gcpfunctions

import (
	"net/http"

	"cloud.google.com/go/functions/apiv2/functionspb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

// SetLogSink wires Cloud Logging: container output and invocations are written to sink; nil disables it.
// Only the 2nd-gen environment exists here, so entries use the cloud_run_revision resource.
func (b *InMemoryBackend) SetLogSink(sink gcpserve.LogSink) { b.logs.SetSink(sink) }

func functionLogTarget(fn *functionspb.Function, instance string) gcpserve.LogTarget {
	n, err := parseName(fn.GetName())
	if err != nil {
		return gcpserve.LogTarget{}
	}

	t := gcpserve.LogTarget{
		Project:  n.project,
		Resource: gcpserve.CloudRunResource(n.project, n.id, fn.GetServiceConfig().GetRevision(), n.location),
	}

	if instance != "" {
		t.Labels = map[string]string{"instanceId": instance}
	}

	return t
}

func (b *InMemoryBackend) captureOutput(r Runner, id string, fn *functionspb.Function) {
	if t := functionLogTarget(fn, id); t.Resource != nil {
		b.logs.Start(b.baseCtx(), r, id, t)
	}
}

func (b *InMemoryBackend) invokeTarget(name, instance string) gcpserve.LogTarget {
	b.mu.RLock("invokeTarget")
	defer b.mu.RUnlock()

	rec := b.functions[name]
	if rec == nil {
		return gcpserve.LogTarget{}
	}

	return functionLogTarget(rec.pb, instance)
}

// serveLogged proxies the invocation to port and writes its request log entry.
func (b *InMemoryBackend) serveLogged(w http.ResponseWriter, r *http.Request, port int, t gcpserve.LogTarget) {
	if t.Resource == nil {
		gcpserve.Proxy(port, b.transport).ServeHTTP(w, r)

		return
	}

	rec := gcpserve.NewRequestRecorder()
	proxy := gcpserve.Proxy(port, b.transport)

	rec.Attach(proxy)

	start := b.now()

	proxy.ServeHTTP(w, r)
	b.logs.Request(b.baseCtx(), t, r, rec, b.now().Sub(start))
}
