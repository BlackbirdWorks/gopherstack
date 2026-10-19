package gcpfirestore

import "cloud.google.com/go/firestore/apiv1/firestorepb"

// API exposes the Firestore service for in-process callers (dashboard proxy).
func (h *Handler) API() firestorepb.FirestoreServer { return &server{b: h.Backend} }
