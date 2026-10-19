package gcpscheduler

import "cloud.google.com/go/scheduler/apiv1/schedulerpb"

// API exposes the CloudScheduler service for in-process callers (dashboard proxy).
func (h *Handler) API() schedulerpb.CloudSchedulerServer { return &server{b: h.Backend} }
