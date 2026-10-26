package gcprun

import (
	"net/http"
	"strconv"
	"time"

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

func (b *InMemoryBackend) captureOutput(runner Runner, id string, rev *runpb.Revision, instance string) {
	if t := revisionLogTarget(rev, instance); t.Resource != nil {
		b.logs.Start(b.baseCtx(), runner, id, t)
	}
}

// serveLogged proxies the request to port and writes its request log entry.
func (b *InMemoryBackend) serveLogged(w http.ResponseWriter, r *http.Request, port int, t gcpserve.LogTarget) {
	if t.Resource == nil {
		b.proxy(port).ServeHTTP(w, r)

		return
	}

	rec := gcpserve.NewRequestRecorder()
	proxy := b.proxy(port)

	rec.Attach(proxy)

	start := b.clk().Now()

	proxy.ServeHTTP(w, r)
	b.logs.Request(b.baseCtx(), t, r, rec, b.clk().Now().Sub(start))
}

const logDrainTimeout = 5 * time.Second

// taskLogTarget is where a job task attempt's output goes: the cloud_run_job resource with the execution, task and
// instance labels the Logging docs list for job entries.
func taskLogTarget(ex *runpb.Execution, t *runpb.Task, attemptNo int, instance string) gcpserve.LogTarget {
	n, err := parseName(ex.GetJob(), "jobs")
	if err != nil {
		return gcpserve.LogTarget{}
	}

	return gcpserve.LogTarget{
		Project:  n.project,
		Resource: gcpserve.CloudRunJobResource(n.project, n.ids[0], n.location),
		Labels: map[string]string{
			"execution_name": lastSegment(ex.GetName()),
			"task_index":     strconv.Itoa(int(t.GetIndex())),
			"task_attempt":   strconv.Itoa(attemptNo),
			"instanceId":     instance,
		},
	}
}

// followTask streams a task container's output to Cloud Logging; the returned stop drains it once the container exited.
func (b *InMemoryBackend) followTask(
	runner Runner, id string, ex *runpb.Execution, t *runpb.Task, attemptNo int,
) func() {
	target := taskLogTarget(ex, t, attemptNo, id)
	if target.Resource == nil {
		return func() {}
	}

	stop := b.logs.Follow(b.baseCtx(), runner, id, target)

	return func() { stop(logDrainTimeout) }
}
