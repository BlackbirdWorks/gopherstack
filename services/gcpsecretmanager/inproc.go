package gcpsecretmanager

import "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"

// API exposes the SecretManagerService for in-process callers (dashboard proxy).
func (h *Handler) API() secretmanagerpb.SecretManagerServiceServer { return &server{b: h.Backend} }
