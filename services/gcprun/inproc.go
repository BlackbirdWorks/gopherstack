package gcprun

import "cloud.google.com/go/run/apiv2/runpb"

// API groups the Cloud Run Admin servers for in-process callers (dashboard proxy).
type API struct {
	Services   runpb.ServicesServer
	Revisions  runpb.RevisionsServer
	Jobs       runpb.JobsServer
	Executions runpb.ExecutionsServer
	Tasks      runpb.TasksServer
}

// API exposes the Cloud Run servers for in-process callers.
func (h *Handler) API() *API {
	return &API{
		Services:   &servicesServer{b: h.Backend},
		Revisions:  &revisionsServer{b: h.Backend},
		Jobs:       &jobsServer{b: h.Backend},
		Executions: &executionsServer{b: h.Backend},
		Tasks:      &tasksServer{b: h.Backend},
	}
}
