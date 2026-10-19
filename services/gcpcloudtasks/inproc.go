package gcpcloudtasks

import "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"

// API exposes the CloudTasks service for in-process callers (dashboard proxy).
func (h *Handler) API() cloudtaskspb.CloudTasksServer { return &server{b: h.Backend} }
