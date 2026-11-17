package ocistreaming

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func (c *kconn) dispatch(req kmsg.Request) kmsg.Response {
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
		return c.fetch(c.k.ctx, r)
	case *kmsg.ListOffsetsRequest:
		return c.listOffsets(r)
	case *kmsg.FindCoordinatorRequest:
		return c.findCoordinator(r)
	case *kmsg.CreateTopicsRequest:
		return c.createTopics(r)
	case *kmsg.DeleteTopicsRequest:
		return c.deleteTopics(r)
	}

	return c.dispatchGroup(req)
}

func (c *kconn) dispatchGroup(req kmsg.Request) kmsg.Response {
	ctx := c.k.ctx

	switch r := req.(type) {
	case *kmsg.JoinGroupRequest:
		if r.Group == "" || len(r.Group) > kafkaMaxGroupName {
			return joinErr(kerrInvalidGroupID)
		}

		return c.k.groups.join(ctx, groupMapKey(c.poolID, r.Group), r)
	case *kmsg.SyncGroupRequest:
		return c.k.groups.sync(ctx, groupMapKey(c.poolID, r.Group), r)
	case *kmsg.HeartbeatRequest:
		return &kmsg.HeartbeatResponse{
			ErrorCode: c.k.groups.heartbeat(groupMapKey(c.poolID, r.Group), r.MemberID, r.Generation),
		}
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
		c.closeAfter = true

		return resp
	}

	parts := strings.Split(string(r.SASLAuthBytes), "\x00")
	if len(parts) == saslPlainParts {
		if pool, ok := c.k.h.Backend.kafkaAuth(parts[1], parts[2], c.k.h.EnforceAuth); ok {
			c.poolID, c.authed = pool, true

			return resp
		}
	}

	msg := "Authentication failed: invalid credentials"
	resp.ErrorCode, resp.ErrorMessage = kerrSASLAuthFailed, &msg
	c.closeAfter = true

	return resp
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

	for _, t := range r.Topics {
		if t.Topic != nil && len(names) < kafkaTopicsPerReq {
			names = append(names, *t.Topic)
		}
	}

	if r.AllowAutoTopicCreation {
		for _, n := range names {
			c.k.h.Backend.kafkaAutoCreateIfMissing(c.poolID, n)
		}
	}

	for _, t := range c.k.h.Backend.kafkaTopics(c.poolID, names, r.Topics == nil) {
		name := t.Name
		mt := kmsg.MetadataResponseTopic{Topic: &name, ErrorCode: t.Code}

		for i := range t.Parts {
			mt.Partitions = append(mt.Partitions, kmsg.MetadataResponseTopicPartition{
				Partition: int32(
					i,
				),
				Leader:   kafkaNodeID,
				Replicas: []int32{kafkaNodeID},
				ISR:      []int32{kafkaNodeID},
			})
		}

		resp.Topics = append(resp.Topics, mt)
	}

	return resp
}

func (c *kconn) findCoordinator(r *kmsg.FindCoordinatorRequest) kmsg.Response {
	host, port := c.advertised()
	resp := &kmsg.FindCoordinatorResponse{NodeID: kafkaNodeID, Host: host, Port: port}

	if r.CoordinatorType != 0 {
		resp.ErrorCode = kerrCoordinatorNA
	}

	return resp
}

func (c *kconn) produce(r *kmsg.ProduceRequest) kmsg.Response {
	resp := &kmsg.ProduceResponse{}
	budget := int64(maxBatchInflated)

	for _, t := range r.Topics {
		rt := kmsg.ProduceResponseTopic{Topic: t.Topic}

		for _, p := range t.Partitions {
			rp := kmsg.ProduceResponseTopicPartition{
				Partition: p.Partition, BaseOffset: -1, LogAppendTime: -1, LogStartOffset: -1,
			}

			if r.TransactionID != nil {
				rp.ErrorCode = kerrInvalidRequest
			} else {
				rp.ErrorCode, rp.BaseOffset = c.appendPartition(t.Topic, int(p.Partition), p.Records, &budget)
			}

			rt.Partitions = append(rt.Partitions, rp)
		}

		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}

func (c *kconn) appendPartition(topic string, part int, data []byte, budget *int64) (int16, int64) {
	msgs, err := splitBatches(data, budget)
	if err != nil {
		switch {
		case errors.Is(err, errCompression):
			return kerrUnsupportedCompress, -1
		case errors.Is(err, errBatchTooLarge):
			return kerrMessageTooLarge, -1
		case errors.Is(err, errUnsupported):
			return kerrInvalidRequest, -1
		}

		return kerrCorrupt, -1
	}

	base, code := c.k.h.Backend.kafkaAppend(c.poolID, topic, part, msgs)

	return code, base
}

func (c *kconn) fetch(ctx context.Context, r *kmsg.FetchRequest) kmsg.Response {
	wait := min(time.Duration(max(r.MaxWaitMillis, 0))*time.Millisecond, kafkaMaxWait)

	maxBytes := int(r.MaxBytes)
	if maxBytes <= 0 || maxBytes > kafkaFetchMaxBytes {
		maxBytes = kafkaFetchMaxBytes
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	for {
		sig := c.k.h.Backend.appendSignal()
		resp, ready := c.fetchOnce(r, maxBytes)

		if ready || wait == 0 {
			return resp
		}

		select {
		case <-sig:
		case <-timer.C:
			return resp
		case <-ctx.Done():
			return resp
		}
	}
}

func (c *kconn) fetchOnce(r *kmsg.FetchRequest, maxBytes int) (*kmsg.FetchResponse, bool) {
	resp := &kmsg.FetchResponse{}
	got, left := false, maxBytes

	for _, t := range r.Topics {
		rt := kmsg.FetchResponseTopic{Topic: t.Topic}

		for _, p := range t.Partitions {
			rp := kmsg.FetchResponseTopicPartition{
				Partition: p.Partition, HighWatermark: -1, LastStableOffset: -1, LogStartOffset: -1,
				PreferredReadReplica: -1,
			}

			res := c.k.h.Backend.kafkaFetch(
				c.poolID,
				t.Topic,
				int(p.Partition),
				p.FetchOffset,
				min(left, int(max(p.PartitionMaxBytes, 1))),
			)
			rp.ErrorCode, rp.HighWatermark, rp.LastStableOffset, rp.LogStartOffset = res.Code, res.End, res.End, res.Start

			if len(res.Msgs) > 0 {
				rp.RecordBatches = encodeBatch(res.Msgs, res.First)
				left = max(left-len(rp.RecordBatches), 1)
				got = true
			}

			got = got || res.Code != kerrNone
			rt.Partitions = append(rt.Partitions, rp)
		}

		resp.Topics = append(resp.Topics, rt)
	}

	return resp, got
}

func (c *kconn) listOffsets(r *kmsg.ListOffsetsRequest) kmsg.Response {
	resp := &kmsg.ListOffsetsResponse{}

	for _, t := range r.Topics {
		rt := kmsg.ListOffsetsResponseTopic{Topic: t.Topic}

		for _, p := range t.Partitions {
			rp := kmsg.ListOffsetsResponseTopicPartition{
				Partition:   p.Partition,
				Timestamp:   -1,
				Offset:      -1,
				LeaderEpoch: -1,
			}
			off, ts, code := c.k.h.Backend.kafkaOffsetAt(c.poolID, t.Topic, int(p.Partition), p.Timestamp)
			rp.ErrorCode, rp.Offset, rp.Timestamp = code, off, ts

			rt.Partitions = append(rt.Partitions, rp)
		}

		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}

func (c *kconn) leaveGroup(r *kmsg.LeaveGroupRequest) kmsg.Response {
	key := groupMapKey(c.poolID, r.Group)
	resp := &kmsg.LeaveGroupResponse{}

	if r.Version < leaveMembersVersion {
		resp.ErrorCode = c.k.groups.leave(key, []string{r.MemberID})[0]

		return resp
	}

	ids := make([]string, 0, len(r.Members))
	for _, m := range r.Members {
		ids = append(ids, m.MemberID)
	}

	for i, code := range c.k.groups.leave(key, ids) {
		resp.Members = append(resp.Members, kmsg.LeaveGroupResponseMember{MemberID: ids[i], ErrorCode: code})
	}

	return resp
}

func (c *kconn) offsetCommit(r *kmsg.OffsetCommitRequest) kmsg.Response {
	resp := &kmsg.OffsetCommitResponse{}

	groupCode := c.k.groups.commitAllowed(groupMapKey(c.poolID, r.Group), r.MemberID, r.Generation)
	if r.Group == "" || len(r.Group) > kafkaMaxGroupName {
		groupCode = kerrInvalidGroupID
	}

	for _, t := range r.Topics {
		rt := kmsg.OffsetCommitResponseTopic{Topic: t.Topic}

		for _, p := range t.Partitions {
			code := groupCode
			if code == kerrNone {
				code = c.k.h.Backend.kafkaCommit(c.poolID, t.Topic, r.Group, int(p.Partition), p.Offset)
			}

			rt.Partitions = append(
				rt.Partitions,
				kmsg.OffsetCommitResponseTopicPartition{Partition: p.Partition, ErrorCode: code},
			)
		}

		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}

func (c *kconn) offsetFetch(r *kmsg.OffsetFetchRequest) kmsg.Response {
	resp := &kmsg.OffsetFetchResponse{}

	if r.Group == "" || len(r.Group) > kafkaMaxGroupName {
		resp.ErrorCode = kerrInvalidGroupID

		return resp
	}

	for _, t := range r.Topics {
		rt := kmsg.OffsetFetchResponseTopic{Topic: t.Topic}

		for _, pid := range t.Partitions {
			off, code := c.k.h.Backend.kafkaCommitted(c.poolID, t.Topic, r.Group, int(pid))
			rt.Partitions = append(rt.Partitions, kmsg.OffsetFetchResponseTopicPartition{
				Partition: pid, Offset: off, LeaderEpoch: -1, ErrorCode: code,
			})
		}

		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}

const (
	saslPlainParts      = 3
	leaveMembersVersion = 3
)

const msPerHour = int64(time.Hour / time.Millisecond)

func (c *kconn) createTopics(r *kmsg.CreateTopicsRequest) kmsg.Response {
	resp := &kmsg.CreateTopicsResponse{}

	for i, t := range r.Topics {
		rt := kmsg.CreateTopicsResponseTopic{Topic: t.Topic, NumPartitions: -1, ReplicationFactor: -1}

		switch {
		case i >= kafkaTopicsPerReq:
			rt.ErrorCode = kerrInvalidRequest
		default:
			rt.ErrorCode = c.createTopic(t, r.ValidateOnly)
		}

		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}

func (c *kconn) createTopic(t kmsg.CreateTopicsRequestTopic, validateOnly bool) int16 {
	in := CreateStreamIn{Name: t.Topic, PoolID: c.poolID}

	if t.NumPartitions > 0 {
		in.Partitions = new(int(t.NumPartitions))
	}

	for _, kc := range t.Configs {
		if kc.Name != "retention.ms" || kc.Value == nil {
			continue
		}

		ms, err := strconv.ParseInt(*kc.Value, 10, 64)
		if err != nil || ms <= 0 {
			return kerrInvalidConfig
		}

		in.RetentionInHours = new(int((ms + msPerHour - 1) / msPerHour))
	}

	return c.k.h.Backend.kafkaCreateTopic(in, validateOnly)
}

func (c *kconn) deleteTopics(r *kmsg.DeleteTopicsRequest) kmsg.Response {
	resp := &kmsg.DeleteTopicsResponse{}

	for i, n := range r.TopicNames {
		rt := kmsg.DeleteTopicsResponseTopic{Topic: &r.TopicNames[i]}
		rt.ErrorCode = c.k.h.Backend.kafkaDeleteTopic(c.poolID, n)
		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}
