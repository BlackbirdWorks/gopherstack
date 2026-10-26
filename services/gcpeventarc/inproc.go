package gcpeventarc

import "cloud.google.com/go/eventarc/apiv1/eventarcpb"

// API exposes the Eventarc service for in-process callers (dashboard proxy).
func (h *Handler) API() eventarcpb.EventarcServer { return &eventarcServer{b: h.Backend} }
