package gcpkms

import "cloud.google.com/go/kms/apiv1/kmspb"

// API exposes the KeyManagementService for in-process callers (dashboard proxy).
func (h *Handler) API() kmspb.KeyManagementServiceServer { return &server{b: h.Backend} }
