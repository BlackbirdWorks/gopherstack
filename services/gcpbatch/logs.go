package gcpbatch

import (
	"context"
	"errors"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/genproto/googleapis/api/monitoredres"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

const taskLogID = "batch_task_logs"

var errUnsupportedRunnable = errors.New("runnable is neither a container nor a script")

// taskLogSink files container output under the Batch task log instead of the Cloud Run log id.
type taskLogSink struct {
	inner   gcpserve.LogSink
	project string
}

func (s taskLogSink) WriteLogEntries(ctx context.Context, req *loggingpb.WriteLogEntriesRequest) error {
	for _, e := range req.GetEntries() {
		e.LogName = gcpserve.LogName(s.project, taskLogID)
	}

	return s.inner.WriteLogEntries(ctx, req) //nolint:wrapcheck // sink errors are dropped by the caller
}

func taskLogResource(ex *execEnv) *monitoredres.MonitoredResource {
	return &monitoredres.MonitoredResource{
		Type: "generic_task",
		Labels: map[string]string{
			"project_id": ex.project, "location": ex.location,
			"job": ex.job[len(locationName(ex.project, ex.location))+len("/"+kindJobs+"/"):],
		},
	}
}
