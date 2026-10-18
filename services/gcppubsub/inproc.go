package gcppubsub

import "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

// PublisherAPI exposes the Publisher service for in-process callers (dashboard proxy).
func (h *Handler) PublisherAPI() pubsubpb.PublisherServer { return &publisherServer{b: h.Backend} }

// SubscriberAPI exposes the Subscriber service for in-process callers (dashboard proxy).
func (h *Handler) SubscriberAPI() pubsubpb.SubscriberServer { return &subscriberServer{b: h.Backend} }
