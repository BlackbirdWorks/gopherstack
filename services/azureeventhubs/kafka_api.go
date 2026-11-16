package azureeventhubs

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	kafkaClusterID      = "gopherstack-eventhubs"
	kafkaDefaultMaxWait = 500 * time.Millisecond
	kafkaMaxWait        = 30 * time.Second
	kafkaMaxFetchBytes  = 8 << 20
	kafkaTopicsPerReq   = 1000
	decimalBase         = 10

	findCoordinatorBatchVersion int16 = 4
)

func (c *kconn) dispatch(hdr reqHeader, req kmsg.Request) kmsg.Response {
	ctx := c.k.ctx

	switch r := req.(type) {
	case *kmsg.ApiVersionsRequest:
		return &kmsg.ApiVersionsResponse{ApiKeys: kafkaVersionList()}
	case *kmsg.SASLHandshakeRequest:
		return c.handshake(r)
	case *kmsg.SASLAuthenticateRequest:
		return c.authenticate(r)
	case *kmsg.MetadataRequest:
		return c.metadata(r)
	case *kmsg.ProduceRequest:
		return c.produce(r)
	case *kmsg.FetchRequest:
		return c.fetch(ctx, r)
	case *kmsg.ListOffsetsRequest:
		return c.listOffsets(r)
	case *kmsg.FindCoordinatorRequest:
		return c.findCoordinator(r)
	case *kmsg.InitProducerIDRequest:
		return c.initProducerID(r)
	case *kmsg.CreateTopicsRequest:
		return c.createTopics(r)
	case *kmsg.DeleteTopicsRequest:
		return c.deleteTopics(r)
	}

	return c.dispatchGroup(ctx, hdr, req)
}

func (c *kconn) dispatchGroup(ctx context.Context, hdr reqHeader, req kmsg.Request) kmsg.Response {
	switch r := req.(type) {
	case *kmsg.JoinGroupRequest:
		return c.joinGroup(ctx, hdr, r)
	case *kmsg.SyncGroupRequest:
		return c.syncGroup(ctx, r)
	case *kmsg.HeartbeatRequest:
		return c.heartbeat(r)
	case *kmsg.LeaveGroupRequest:
		return c.leaveGroup(r)
	case *kmsg.OffsetCommitRequest:
		return c.offsetCommit(r)
	case *kmsg.OffsetFetchRequest:
		return c.offsetFetch(r)
	}

	return nil
}

func (c *kconn) handshake(r *kmsg.SASLHandshakeRequest) kmsg.Response {
	resp := &kmsg.SASLHandshakeResponse{SupportedMechanisms: []string{kafkaMechanism}}

	switch {
	case c.mech || c.authed:
		resp.ErrorCode = kerrIllegalSASLState
	case r.Mechanism != kafkaMechanism:
		resp.ErrorCode = kerrUnsupportedSASL
	default:
		c.mech = true
	}

	return resp
}

func (c *kconn) authenticate(r *kmsg.SASLAuthenticateRequest) kmsg.Response {
	resp := &kmsg.SASLAuthenticateResponse{}

	if !c.mech || c.authed {
		resp.ErrorCode = kerrIllegalSASLState

		return resp
	}

	parts := strings.Split(string(r.SASLAuthBytes), "\x00")
	if len(parts) != 3 || !c.credentials(parts[1], parts[2]) {
		msg := "Authentication failed: invalid credentials"
		resp.ErrorCode, resp.ErrorMessage = kerrSASLAuthFailed, &msg
		c.closeAfter = true

		return resp
	}

	c.authed = true

	return resp
}

// credentials accepts the Event Hubs form (user $ConnectionString, password a connection string) and a plain
// rule name with its key. Without SAS validation any well-formed credentials are accepted, as on the AMQP plane.
func (c *kconn) credentials(user, password string) bool {
	name, key := user, password

	if user == kafkaConnUser {
		c.ns, name, key = ParseConnectionString(password)
		if !c.k.h.Backend.NamespaceExists(c.ns) {
			c.ns = ""
		}
	}

	if !c.k.h.ValidateSAS {
		return true
	}

	rights, scope, ok := c.k.h.Backend.RuleRights(c.ns, name, key)
	if !ok {
		return false
	}

	c.rights, c.hubScope, c.user = rights, scope, name

	return true
}

// allowed reports whether the connection's rule grants right on hub.
func (c *kconn) allowed(right, hub string) bool {
	if !c.k.h.ValidateSAS {
		return true
	}

	return hasRight(c.rights, right) && (c.hubScope == "" || c.hubScope == hub)
}

func (c *kconn) hubRef(topic string) (HubRef, bool) {
	if c.ns != "" {
		ref := HubRef{Namespace: c.ns, Hub: topic}
		_, err := c.k.h.Backend.Hub(ref)

		return ref, err == nil
	}

	return c.k.h.Backend.ResolveHub(topic)
}

func (c *kconn) metadata(r *kmsg.MetadataRequest) kmsg.Response {
	host, port := c.advertised()
	cluster := kafkaClusterID
	resp := &kmsg.MetadataResponse{
		Brokers:      []kmsg.MetadataResponseBroker{{NodeID: kafkaNodeID, Host: host, Port: port}},
		ClusterID:    &cluster,
		ControllerID: kafkaNodeID,
	}

	var names []string

	if r.Topics == nil {
		for _, n := range c.k.h.Backend.HubNames(c.ns) {
			if c.allowed(RightListen, n) || c.allowed(RightSend, n) {
				names = append(names, n)
			}
		}
	} else {
		for _, t := range r.Topics {
			if t.Topic != nil {
				names = append(names, *t.Topic)
			}
		}
	}

	if len(names) > kafkaTopicsPerReq {
		names = names[:kafkaTopicsPerReq]
	}

	for _, n := range names {
		resp.Topics = append(resp.Topics, c.topicMetadata(n))
	}

	return resp
}

func (c *kconn) topicMetadata(name string) kmsg.MetadataResponseTopic {
	t := kmsg.MetadataResponseTopic{Topic: &name}

	ref, ok := c.hubRef(name)
	if !ok {
		t.ErrorCode = kerrUnknownTopic

		return t
	}

	if !c.allowed(RightListen, name) && !c.allowed(RightSend, name) {
		t.ErrorCode = kerrTopicAuthFailed

		return t
	}

	info, err := c.k.h.Backend.Hub(ref)
	if err != nil {
		t.ErrorCode = kerrUnknownTopic

		return t
	}

	for i := range info.PartitionIDs {
		t.Partitions = append(t.Partitions, kmsg.MetadataResponseTopicPartition{
			Partition: int32(i), Leader: kafkaNodeID, LeaderEpoch: 0,
			Replicas: []int32{kafkaNodeID}, ISR: []int32{kafkaNodeID},
		})
	}

	return t
}

func (c *kconn) findCoordinator(r *kmsg.FindCoordinatorRequest) kmsg.Response {
	host, port := c.advertised()
	resp := &kmsg.FindCoordinatorResponse{NodeID: kafkaNodeID, Host: host, Port: port}

	one := func(key string) kmsg.FindCoordinatorResponseCoordinator {
		co := kmsg.FindCoordinatorResponseCoordinator{Key: key, NodeID: kafkaNodeID, Host: host, Port: port}
		if r.CoordinatorType != 0 {
			co.ErrorCode = kerrCoordinatorUnavailable
		}

		return co
	}

	if r.Version >= findCoordinatorBatchVersion {
		for _, k := range r.CoordinatorKeys {
			resp.Coordinators = append(resp.Coordinators, one(k))
		}

		return resp
	}

	resp.ErrorCode = one(r.CoordinatorKey).ErrorCode

	return resp
}

func (c *kconn) initProducerID(r *kmsg.InitProducerIDRequest) kmsg.Response {
	if r.TransactionalID != nil {
		return &kmsg.InitProducerIDResponse{ErrorCode: kerrInvalidRequest, ProducerID: -1, ProducerEpoch: -1}
	}

	id, epoch := c.k.prod.allocate(r.ProducerID, r.ProducerEpoch)

	return &kmsg.InitProducerIDResponse{ProducerID: id, ProducerEpoch: epoch}
}

func (c *kconn) createTopics(r *kmsg.CreateTopicsRequest) kmsg.Response {
	resp := &kmsg.CreateTopicsResponse{}

	for _, t := range r.Topics {
		rt := kmsg.CreateTopicsResponseTopic{Topic: t.Topic, NumPartitions: -1, ReplicationFactor: -1}
		rt.ErrorCode = c.createTopic(t, r.ValidateOnly)
		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}

func (c *kconn) createTopic(t kmsg.CreateTopicsRequestTopic, validateOnly bool) int16 {
	ns := c.ns
	if ns == "" {
		names := c.k.h.Backend.NamespaceNames()
		if len(names) == 0 {
			return kerrInvalidRequest
		}

		ns = names[0]
	}

	if !c.allowed(RightManage, t.Topic) {
		return kerrTopicAuthFailed
	}

	cfg := HubConfig{PartitionCount: int(t.NumPartitions)}

	if t.NumPartitions <= 0 {
		cfg.PartitionCount = DefaultPartitionCount
	}

	for _, kc := range t.Configs {
		if kc.Name == "retention.ms" && kc.Value != nil {
			ms, ok := parseMillis(*kc.Value)
			if !ok {
				return kerrInvalidRequest
			}

			cfg.Retention = ms
		}
	}

	ref := HubRef{Namespace: ns, Hub: t.Topic}
	if _, err := c.k.h.Backend.Hub(ref); err == nil {
		return kerrTopicAlreadyExists
	}

	if validateOnly {
		return kerrNone
	}

	if _, err := c.k.h.Backend.CreateHub(ref, cfg); err != nil {
		return topicErr(err)
	}

	return kerrNone
}

func parseMillis(s string) (time.Duration, bool) {
	var ms int64

	for _, ch := range s {
		if ch < '0' || ch > '9' || ms > 1<<50 {
			return 0, false
		}

		ms = ms*decimalBase + int64(ch-'0')
	}

	return time.Duration(ms) * time.Millisecond, s != ""
}

func topicErr(err error) int16 {
	switch {
	case err == nil:
		return kerrNone
	case errors.Is(err, ErrInvalidPartitionCount):
		return kerrInvalidPartitions
	case errors.Is(err, ErrInvalidName), errors.Is(err, ErrInvalidRetention):
		return kerrInvalidRequest
	case errors.Is(err, ErrHubNotFound), errors.Is(err, ErrNamespaceNotFound):
		return kerrUnknownTopic
	}

	return kerrInvalidRequest
}

func (c *kconn) deleteTopics(r *kmsg.DeleteTopicsRequest) kmsg.Response {
	resp := &kmsg.DeleteTopicsResponse{}

	names := slices.Clone(r.TopicNames)
	for _, t := range r.Topics {
		if t.Topic != nil {
			names = append(names, *t.Topic)
		}
	}

	for _, n := range names {
		rt := kmsg.DeleteTopicsResponseTopic{Topic: &n}

		ref, ok := c.hubRef(n)

		switch {
		case !ok:
			rt.ErrorCode = kerrUnknownTopic
		case !c.allowed(RightManage, n):
			rt.ErrorCode = kerrTopicAuthFailed
		default:
			rt.ErrorCode = topicErr(c.k.h.Backend.DeleteHub(ref))
		}

		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}
