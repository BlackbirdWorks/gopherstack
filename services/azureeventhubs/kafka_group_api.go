package azureeventhubs

import (
	"context"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// groupAllowed reports whether the connection may use consumer groups (Listen right).
func (c *kconn) groupAllowed() bool {
	return !c.k.h.ValidateSAS || hasRight(c.rights, RightListen)
}

func (c *kconn) gkey(group string) string { return groupKey(c.ns, group) }

func (c *kconn) joinGroup(ctx context.Context, _ reqHeader, r *kmsg.JoinGroupRequest) kmsg.Response {
	switch {
	case r.Group == "":
		return joinError(kerrInvalidGroupID)
	case !c.groupAllowed():
		return joinError(kerrGroupAuthFailed)
	}

	return c.k.groups.join(ctx, c.gkey(r.Group), r)
}

func (c *kconn) syncGroup(ctx context.Context, r *kmsg.SyncGroupRequest) kmsg.Response {
	switch {
	case r.Group == "":
		return &kmsg.SyncGroupResponse{ErrorCode: kerrInvalidGroupID}
	case !c.groupAllowed():
		return &kmsg.SyncGroupResponse{ErrorCode: kerrGroupAuthFailed}
	}

	return c.k.groups.sync(ctx, c.gkey(r.Group), r)
}

func (c *kconn) heartbeat(r *kmsg.HeartbeatRequest) kmsg.Response {
	switch {
	case r.Group == "":
		return &kmsg.HeartbeatResponse{ErrorCode: kerrInvalidGroupID}
	case !c.groupAllowed():
		return &kmsg.HeartbeatResponse{ErrorCode: kerrGroupAuthFailed}
	}

	return &kmsg.HeartbeatResponse{ErrorCode: c.k.groups.heartbeat(c.gkey(r.Group), r.MemberID, r.Generation)}
}

func (c *kconn) leaveGroup(r *kmsg.LeaveGroupRequest) kmsg.Response {
	switch {
	case r.Group == "":
		return &kmsg.LeaveGroupResponse{ErrorCode: kerrInvalidGroupID}
	case !c.groupAllowed():
		return &kmsg.LeaveGroupResponse{ErrorCode: kerrGroupAuthFailed}
	}

	ids := []string{r.MemberID}
	if r.Version >= leaveMembersVersion {
		ids = ids[:0]
		for _, m := range r.Members {
			ids = append(ids, m.MemberID)
		}
	}

	codes := c.k.groups.leave(c.gkey(r.Group), ids)
	resp := &kmsg.LeaveGroupResponse{}

	if r.Version < leaveMembersVersion {
		resp.ErrorCode = codes[0]

		return resp
	}

	for i, id := range ids {
		resp.Members = append(resp.Members, kmsg.LeaveGroupResponseMember{MemberID: id, ErrorCode: codes[i]})
	}

	return resp
}

func (c *kconn) offsetCommit(r *kmsg.OffsetCommitRequest) kmsg.Response {
	resp := &kmsg.OffsetCommitResponse{}

	var groupCode int16

	switch {
	case r.Group == "":
		groupCode = kerrInvalidGroupID
	case !c.groupAllowed():
		groupCode = kerrGroupAuthFailed
	default:
		groupCode = c.k.groups.commitAllowed(c.gkey(r.Group), r.MemberID, r.Generation)
	}

	for _, t := range r.Topics {
		rt := kmsg.OffsetCommitResponseTopic{Topic: t.Topic}
		ref, found := c.hubRef(t.Topic)

		for _, p := range t.Partitions {
			code := groupCode

			switch {
			case code != kerrNone:
			case !found:
				code = kerrUnknownTopic
			case p.Offset < 0:
				code = kerrOffsetOutOfRange
			default:
				code = topicErr(c.k.h.Backend.CommitKafkaOffset(ref, r.Group, int(p.Partition), p.Offset))
			}

			rt.Partitions = append(rt.Partitions,
				kmsg.OffsetCommitResponseTopicPartition{Partition: p.Partition, ErrorCode: code})
		}

		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}

func (c *kconn) offsetFetch(r *kmsg.OffsetFetchRequest) kmsg.Response {
	resp := &kmsg.OffsetFetchResponse{}

	if r.Group == "" {
		resp.ErrorCode = kerrInvalidGroupID

		return resp
	}

	if !c.groupAllowed() {
		resp.ErrorCode = kerrGroupAuthFailed

		return resp
	}

	topics := r.Topics

	if topics == nil {
		for _, n := range c.k.h.Backend.HubNames(c.ns) {
			ref, ok := c.hubRef(n)
			if !ok {
				continue
			}

			committed, err := c.k.h.Backend.KafkaCommitted(ref, r.Group)
			if err != nil || len(committed) == 0 {
				continue
			}

			rt := kmsg.OffsetFetchRequestTopic{Topic: n}
			for pid := range committed {
				rt.Partitions = append(rt.Partitions, int32(pid)) //nolint:gosec // bounded by MaxPartitionCount
			}

			topics = append(topics, rt)
		}
	}

	for _, t := range topics {
		resp.Topics = append(resp.Topics, c.fetchTopicOffsets(r.Group, t))
	}

	return resp
}

func (c *kconn) fetchTopicOffsets(group string, t kmsg.OffsetFetchRequestTopic) kmsg.OffsetFetchResponseTopic {
	rt := kmsg.OffsetFetchResponseTopic{Topic: t.Topic}
	ref, found := c.hubRef(t.Topic)

	for _, pid := range t.Partitions {
		rp := kmsg.OffsetFetchResponseTopicPartition{Partition: pid, Offset: -1, LeaderEpoch: -1}

		if !found {
			rp.ErrorCode = kerrUnknownTopic
		} else if off, err := c.k.h.Backend.KafkaOffset(ref, group, int(pid)); err != nil {
			rp.ErrorCode = kerrUnknownTopic
		} else {
			rp.Offset = off
		}

		rt.Partitions = append(rt.Partitions, rp)
	}

	return rt
}
