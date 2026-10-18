package dashboard

import (
	"context"
	"net/http"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/proto"
)

const (
	gcpPubSubProxyPrefix = "/dashboard/api/v1/gcp/pubsub"
	gcpPubSubKindIdx     = 2
	gcpPubSubListSegs    = 3
	gcpPubSubItemSegs    = 4
	gcpPubSubSubListSegs = 5
	kindTopics           = "topics"
	kindSubscriptions    = "subscriptions"
	kindSnapshots        = "snapshots"
)

// registerGCPPubSubRoutes exposes Pub/Sub to the SPA as the REST JSON shapes of
// projects.topics/subscriptions/snapshots, calling the backend in-process.
func (h *DashboardHandler) registerGCPPubSubRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPPubSubOps; ops != nil {
		route = (&gcpPubSubAPI{pub: ops.PublisherAPI(), sub: ops.SubscriberAPI()}).route
	}

	h.registerGCPRESTProxy(gcpPubSubProxyPrefix, route)
}

type gcpPubSubAPI struct {
	pub pubsubpb.PublisherServer
	sub pubsubpb.SubscriberServer
}

func (a *gcpPubSubAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n < gcpPubSubListSegs || r.segs[0] != gcpRESTProjectsSeg {
		return nil, errGCPUnsupported()
	}

	switch r.segs[gcpPubSubKindIdx] {
	case kindTopics:
		return a.topics(ctx, r)
	case kindSubscriptions:
		return a.subscriptions(ctx, r)
	case kindSnapshots:
		return a.snapshots(ctx, r)
	default:
		return nil, errGCPUnsupported()
	}
}

func (a *gcpPubSubAPI) topics(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n, parent := len(r.segs), r.name(gcpPubSubKindIdx)

	switch {
	case n == gcpPubSubListSegs && r.method == http.MethodGet && r.verb == "":
		return a.pub.ListTopics(ctx, &pubsubpb.ListTopicsRequest{
			Project: parent, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	case n == gcpPubSubItemSegs:
		return a.topic(ctx, r)
	case n == gcpPubSubSubListSegs && r.verb == "" && r.method == http.MethodGet:
		name := r.name(gcpPubSubItemSegs)

		switch r.segs[gcpPubSubItemSegs] {
		case kindSubscriptions:
			return a.pub.ListTopicSubscriptions(ctx, &pubsubpb.ListTopicSubscriptionsRequest{
				Topic: name, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
			})
		case kindSnapshots:
			return a.pub.ListTopicSnapshots(ctx, &pubsubpb.ListTopicSnapshotsRequest{
				Topic: name, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
			})
		}
	}

	return nil, errGCPUnsupported()
}

func (a *gcpPubSubAPI) topic(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(gcpPubSubItemSegs)

	switch {
	case r.verb == "publish" && r.method == http.MethodPost:
		req := &pubsubpb.PublishRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Topic = name

		return a.pub.Publish(ctx, req)
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodPut:
		t := &pubsubpb.Topic{}
		if err := r.unmarshal(t); err != nil {
			return nil, err
		}

		t.Name = name

		return a.pub.CreateTopic(ctx, t)
	case r.method == http.MethodGet:
		return a.pub.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: name})
	case r.method == http.MethodPatch:
		req := &pubsubpb.UpdateTopicRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		if req.GetTopic() == nil {
			req.Topic = &pubsubpb.Topic{}
		}

		req.Topic.Name = name

		return a.pub.UpdateTopic(ctx, req)
	case r.method == http.MethodDelete:
		return a.pub.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: name})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpPubSubAPI) subscriptions(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)

	if n == gcpPubSubListSegs && r.method == http.MethodGet && r.verb == "" {
		return a.sub.ListSubscriptions(ctx, &pubsubpb.ListSubscriptionsRequest{
			Project: r.name(gcpPubSubKindIdx), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	}

	if n != gcpPubSubItemSegs {
		return nil, errGCPUnsupported()
	}

	name := r.name(gcpPubSubItemSegs)

	if r.verb != "" {
		return a.subscriptionVerb(ctx, r, name)
	}

	switch r.method {
	case http.MethodPut:
		s := &pubsubpb.Subscription{}
		if err := r.unmarshal(s); err != nil {
			return nil, err
		}

		s.Name = name

		return a.sub.CreateSubscription(ctx, s)
	case http.MethodGet:
		return a.sub.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: name})
	case http.MethodPatch:
		req := &pubsubpb.UpdateSubscriptionRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		if req.GetSubscription() == nil {
			req.Subscription = &pubsubpb.Subscription{}
		}

		req.Subscription.Name = name

		return a.sub.UpdateSubscription(ctx, req)
	case http.MethodDelete:
		return a.sub.DeleteSubscription(ctx, &pubsubpb.DeleteSubscriptionRequest{Subscription: name})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpPubSubAPI) subscriptionVerb(ctx context.Context, r gcpRESTCall, name string) (proto.Message, error) {
	if r.method != http.MethodPost {
		return nil, errGCPUnsupported()
	}

	switch r.verb {
	case "pull":
		req := &pubsubpb.PullRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Subscription = name
		req.ReturnImmediately = true //nolint:staticcheck // the dashboard never long-polls

		return a.sub.Pull(ctx, req)
	case "acknowledge":
		req := &pubsubpb.AcknowledgeRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Subscription = name

		return a.sub.Acknowledge(ctx, req)
	case "modifyAckDeadline":
		req := &pubsubpb.ModifyAckDeadlineRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Subscription = name

		return a.sub.ModifyAckDeadline(ctx, req)
	case "modifyPushConfig":
		req := &pubsubpb.ModifyPushConfigRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Subscription = name

		return a.sub.ModifyPushConfig(ctx, req)
	case "seek":
		req := &pubsubpb.SeekRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Subscription = name

		return a.sub.Seek(ctx, req)
	}

	return nil, errGCPUnsupported()
}

func (a *gcpPubSubAPI) snapshots(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)

	if n == gcpPubSubListSegs && r.method == http.MethodGet && r.verb == "" {
		return a.sub.ListSnapshots(ctx, &pubsubpb.ListSnapshotsRequest{
			Project: r.name(gcpPubSubKindIdx), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	}

	if n != gcpPubSubItemSegs || r.verb != "" {
		return nil, errGCPUnsupported()
	}

	name := r.name(gcpPubSubItemSegs)

	switch r.method {
	case http.MethodPut:
		req := &pubsubpb.CreateSnapshotRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Name = name

		return a.sub.CreateSnapshot(ctx, req)
	case http.MethodGet:
		return a.sub.GetSnapshot(ctx, &pubsubpb.GetSnapshotRequest{Snapshot: name})
	case http.MethodDelete:
		return a.sub.DeleteSnapshot(ctx, &pubsubpb.DeleteSnapshotRequest{Snapshot: name})
	}

	return nil, errGCPUnsupported()
}
