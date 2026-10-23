package kafka

import "github.com/twmb/franz-go/pkg/kmsg"

func produceDeny(r *kmsg.ProduceRequest) kmsg.Response {
	resp, _ := r.ResponseKind().(*kmsg.ProduceResponse)

	for _, t := range r.Topics {
		rt := kmsg.NewProduceResponseTopic()
		rt.Topic = t.Topic

		for _, p := range t.Partitions {
			rp := kmsg.NewProduceResponseTopicPartition()
			rp.Partition, rp.ErrorCode = p.Partition, errCodeTopicAuth
			rt.Partitions = append(rt.Partitions, rp)
		}

		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}

func fetchDeny(r *kmsg.FetchRequest) kmsg.Response {
	resp, _ := r.ResponseKind().(*kmsg.FetchResponse)

	for _, t := range r.Topics {
		rt := kmsg.NewFetchResponseTopic()
		rt.Topic = t.Topic

		for _, p := range t.Partitions {
			rp := kmsg.NewFetchResponseTopicPartition()
			rp.Partition, rp.ErrorCode = p.Partition, errCodeTopicAuth
			rt.Partitions = append(rt.Partitions, rp)
		}

		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}

func metadataDeny(r *kmsg.MetadataRequest) kmsg.Response {
	resp, _ := r.ResponseKind().(*kmsg.MetadataResponse)

	for _, t := range r.Topics {
		rt := kmsg.NewMetadataResponseTopic()
		rt.Topic, rt.ErrorCode = t.Topic, errCodeTopicAuth
		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}

func listOffsetsDeny(r *kmsg.ListOffsetsRequest) kmsg.Response {
	resp, _ := r.ResponseKind().(*kmsg.ListOffsetsResponse)

	for _, t := range r.Topics {
		rt := kmsg.NewListOffsetsResponseTopic()
		rt.Topic = t.Topic

		for _, p := range t.Partitions {
			rp := kmsg.NewListOffsetsResponseTopicPartition()
			rp.Partition, rp.ErrorCode = p.Partition, errCodeTopicAuth
			rt.Partitions = append(rt.Partitions, rp)
		}

		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}

func createTopicsDeny(r *kmsg.CreateTopicsRequest) kmsg.Response {
	resp, _ := r.ResponseKind().(*kmsg.CreateTopicsResponse)

	for _, t := range r.Topics {
		rt := kmsg.NewCreateTopicsResponseTopic()
		rt.Topic, rt.ErrorCode = t.Topic, errCodeTopicAuth
		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}

func deleteTopicsDeny(r *kmsg.DeleteTopicsRequest) kmsg.Response {
	resp, _ := r.ResponseKind().(*kmsg.DeleteTopicsResponse)

	for _, name := range deleteTopicsNames(r) {
		rt := kmsg.NewDeleteTopicsResponseTopic()
		rt.Topic, rt.ErrorCode = &name, errCodeTopicAuth
		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}

func initProducerDeny(r *kmsg.InitProducerIDRequest, code int16) kmsg.Response {
	resp, _ := r.ResponseKind().(*kmsg.InitProducerIDResponse)
	resp.ErrorCode = code

	return resp
}

func joinDeny(r *kmsg.JoinGroupRequest) kmsg.Response {
	resp, _ := r.ResponseKind().(*kmsg.JoinGroupResponse)
	resp.ErrorCode = errCodeGroupAuth

	return resp
}

func syncDeny(r *kmsg.SyncGroupRequest) kmsg.Response {
	resp, _ := r.ResponseKind().(*kmsg.SyncGroupResponse)
	resp.ErrorCode = errCodeGroupAuth

	return resp
}

func heartbeatDeny(r *kmsg.HeartbeatRequest) kmsg.Response {
	resp, _ := r.ResponseKind().(*kmsg.HeartbeatResponse)
	resp.ErrorCode = errCodeGroupAuth

	return resp
}

func leaveDeny(r *kmsg.LeaveGroupRequest) kmsg.Response {
	resp, _ := r.ResponseKind().(*kmsg.LeaveGroupResponse)
	resp.ErrorCode = errCodeGroupAuth

	return resp
}

func offsetCommitDeny(r *kmsg.OffsetCommitRequest) kmsg.Response {
	resp, _ := r.ResponseKind().(*kmsg.OffsetCommitResponse)

	for _, t := range r.Topics {
		rt := kmsg.NewOffsetCommitResponseTopic()
		rt.Topic = t.Topic

		for _, p := range t.Partitions {
			rp := kmsg.NewOffsetCommitResponseTopicPartition()
			rp.Partition, rp.ErrorCode = p.Partition, errCodeGroupAuth
			rt.Partitions = append(rt.Partitions, rp)
		}

		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}
