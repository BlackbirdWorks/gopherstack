package gcpiam

import "cloud.google.com/go/iam/admin/apiv1/adminpb"

// API exposes the IAM admin service for in-process callers (dashboard proxy).
func (h *Handler) API() adminpb.IAMServer { return &server{b: h.Backend} }
