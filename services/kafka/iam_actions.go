package kafka

import (
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	kindCluster = "cluster"
	kindTopic   = "topic"
	kindGroup   = "group"
	kindTxn     = "transactional-id"

	actionAlterGroup    = "AlterGroup"
	actionDescribeGroup = "DescribeGroup"

	errCodeTopicAuth   int16 = 29
	errCodeGroupAuth   int16 = 30
	errCodeClusterAuth int16 = 31
	errCodeTxnAuth     int16 = 53

	arnPartCount = 6
)

// access is one kafka-cluster action a Kafka request needs on one resource.
type access struct {
	action string
	kind   string
	name   string
}

// rule is what a request needs. deny builds the protocol-level rejection; nil means the connection is closed.
type rule struct {
	deny     func() kmsg.Response
	accesses []access
	silent   bool
}

// iamAuthorizer reports whether the authenticated principal may perform action on resource.
type iamAuthorizer func(action, resource string) bool

// resourceArn derives the topic/group/transactional-id ARN MSK IAM policies use from the cluster ARN.
func resourceArn(clusterArn, kind, name string) string {
	if kind == kindCluster {
		return clusterArn
	}

	parts := strings.SplitN(clusterArn, ":", arnPartCount)
	if len(parts) != arnPartCount {
		return clusterArn
	}

	_, rest, _ := strings.Cut(parts[arnPartCount-1], "/")

	return arn.Build("kafka", parts[3], parts[4], kind+"/"+rest+"/"+name)
}

// allowed reports whether every access of r is permitted.
func (r rule) allowed(authz iamAuthorizer, clusterArn string) bool {
	for _, a := range r.accesses {
		if a.kind != kindCluster && a.name == "" {
			return false
		}

		if !authz(iamAuthService+":"+a.action, resourceArn(clusterArn, a.kind, a.name)) {
			return false
		}
	}

	return true
}

func topicAccesses(action string, names ...string) []access {
	out := make([]access, 0, len(names))
	for _, n := range names {
		out = append(out, access{action: action, kind: kindTopic, name: n})
	}

	return out
}

// ruleFor maps a decoded request to the IAM actions it needs; the bool is false for requests MSK does not gate.
func ruleFor(req kmsg.Request) (rule, bool) {
	if r, ok := topicRuleFor(req); ok {
		return r, true
	}

	return groupRuleFor(req)
}

func topicRuleFor(req kmsg.Request) (rule, bool) {
	switch r := req.(type) {
	case *kmsg.ProduceRequest:
		return produceRule(r), true
	case *kmsg.FetchRequest:
		return fetchRule(r), true
	case *kmsg.MetadataRequest:
		return metadataRule(r), true
	case *kmsg.ListOffsetsRequest:
		return listOffsetsRule(r), true
	case *kmsg.CreateTopicsRequest:
		return createTopicsRule(r), true
	case *kmsg.DeleteTopicsRequest:
		return deleteTopicsRule(r), true
	case *kmsg.CreatePartitionsRequest:
		return createPartitionsRule(r), true
	case *kmsg.InitProducerIDRequest:
		return initProducerRule(r), true
	case *kmsg.DescribeConfigsRequest:
		return configRule(r.Resources, describeConfigRes, "DescribeTopicDynamicConfiguration",
			"DescribeClusterDynamicConfiguration"), true
	case *kmsg.IncrementalAlterConfigsRequest:
		return configRule(r.Resources, incrementalConfigRes, "AlterTopicDynamicConfiguration",
			"AlterClusterDynamicConfiguration"), true
	case *kmsg.AlterConfigsRequest:
		return configRule(r.Resources, alterConfigRes, "AlterTopicDynamicConfiguration",
			"AlterClusterDynamicConfiguration"), true
	}

	return rule{}, false
}

func groupRuleFor(req kmsg.Request) (rule, bool) {
	switch r := req.(type) {
	case *kmsg.JoinGroupRequest:
		return groupRule(r.Group, func() kmsg.Response { return joinDeny(r) }), true
	case *kmsg.SyncGroupRequest:
		return groupRule(r.Group, func() kmsg.Response { return syncDeny(r) }), true
	case *kmsg.HeartbeatRequest:
		return groupRule(r.Group, func() kmsg.Response { return heartbeatDeny(r) }), true
	case *kmsg.LeaveGroupRequest:
		return groupRule(r.Group, func() kmsg.Response { return leaveDeny(r) }), true
	case *kmsg.OffsetCommitRequest:
		return offsetCommitRule(r), true
	case *kmsg.OffsetFetchRequest:
		return offsetFetchRule(r), true
	case *kmsg.DescribeGroupsRequest:
		return describeGroupsRule(r), true
	case *kmsg.DeleteGroupsRequest:
		return deleteGroupsRule(r), true
	}

	return rule{}, false
}

func groupRule(group string, deny func() kmsg.Response) rule {
	return rule{accesses: []access{{action: actionAlterGroup, kind: kindGroup, name: group}}, deny: deny}
}

func produceRule(r *kmsg.ProduceRequest) rule {
	names := make([]string, 0, len(r.Topics))
	for _, t := range r.Topics {
		names = append(names, t.Topic)
	}

	return rule{
		accesses: topicAccesses("WriteData", names...),
		deny:     func() kmsg.Response { return produceDeny(r) },
		silent:   r.Acks == 0,
	}
}

func fetchRule(r *kmsg.FetchRequest) rule {
	names := make([]string, 0, len(r.Topics))
	for _, t := range r.Topics {
		names = append(names, t.Topic)
	}

	return rule{accesses: topicAccesses("ReadData", names...), deny: func() kmsg.Response { return fetchDeny(r) }}
}

func metadataRule(r *kmsg.MetadataRequest) rule {
	names := make([]string, 0, len(r.Topics))
	for _, t := range r.Topics {
		names = append(names, ptrOrEmpty(t.Topic))
	}

	return rule{
		accesses: topicAccesses("DescribeTopic", names...),
		deny:     func() kmsg.Response { return metadataDeny(r) },
	}
}

func listOffsetsRule(r *kmsg.ListOffsetsRequest) rule {
	names := make([]string, 0, len(r.Topics))
	for _, t := range r.Topics {
		names = append(names, t.Topic)
	}

	return rule{
		accesses: topicAccesses("DescribeTopic", names...),
		deny:     func() kmsg.Response { return listOffsetsDeny(r) },
	}
}

func createTopicsRule(r *kmsg.CreateTopicsRequest) rule {
	names := make([]string, 0, len(r.Topics))
	for _, t := range r.Topics {
		names = append(names, t.Topic)
	}

	return rule{
		accesses: topicAccesses("CreateTopic", names...),
		deny:     func() kmsg.Response { return createTopicsDeny(r) },
	}
}

func deleteTopicsNames(r *kmsg.DeleteTopicsRequest) []string {
	names := slicesCloneStrings(r.TopicNames)
	for _, t := range r.Topics {
		names = append(names, ptrOrEmpty(t.Topic))
	}

	return names
}

func deleteTopicsRule(r *kmsg.DeleteTopicsRequest) rule {
	return rule{
		accesses: topicAccesses("DeleteTopic", deleteTopicsNames(r)...),
		deny:     func() kmsg.Response { return deleteTopicsDeny(r) },
	}
}

func createPartitionsRule(r *kmsg.CreatePartitionsRequest) rule {
	names := make([]string, 0, len(r.Topics))
	for _, t := range r.Topics {
		names = append(names, t.Topic)
	}

	return rule{accesses: topicAccesses("AlterTopic", names...)}
}

func initProducerRule(r *kmsg.InitProducerIDRequest) rule {
	if r.TransactionalID != nil {
		return rule{
			accesses: []access{{action: "AlterTransactionalId", kind: kindTxn, name: *r.TransactionalID}},
			deny:     func() kmsg.Response { return initProducerDeny(r, errCodeTxnAuth) },
		}
	}

	return rule{
		accesses: []access{{action: "WriteDataIdempotently", kind: kindCluster}},
		deny:     func() kmsg.Response { return initProducerDeny(r, errCodeClusterAuth) },
	}
}

func offsetCommitRule(r *kmsg.OffsetCommitRequest) rule {
	acc := make([]access, 0, 1+len(r.Topics))
	acc = append(acc, access{action: actionAlterGroup, kind: kindGroup, name: r.Group})

	for _, t := range r.Topics {
		acc = append(acc, access{action: "ReadData", kind: kindTopic, name: t.Topic})
	}

	return rule{accesses: acc, deny: func() kmsg.Response { return offsetCommitDeny(r) }}
}

func offsetFetchRule(r *kmsg.OffsetFetchRequest) rule {
	groups := make([]access, 0, len(r.Groups)+1)
	if r.Version < offsetFetchGroupsVersion {
		groups = append(groups, access{action: actionDescribeGroup, kind: kindGroup, name: r.Group})
	}

	for _, g := range r.Groups {
		groups = append(groups, access{action: actionDescribeGroup, kind: kindGroup, name: g.Group})
	}

	return rule{accesses: groups}
}

func describeGroupsRule(r *kmsg.DescribeGroupsRequest) rule {
	acc := make([]access, 0, len(r.Groups))
	for _, g := range r.Groups {
		acc = append(acc, access{action: actionDescribeGroup, kind: kindGroup, name: g})
	}

	return rule{accesses: acc}
}

func deleteGroupsRule(r *kmsg.DeleteGroupsRequest) rule {
	acc := make([]access, 0, len(r.Groups))
	for _, g := range r.Groups {
		acc = append(acc, access{action: "DeleteGroup", kind: kindGroup, name: g})
	}

	return rule{accesses: acc}
}

const (
	offsetFetchGroupsVersion = 8
	configResourceTopic      = 2
	configResourceBroker     = 4
)

type configRes struct {
	name string
	typ  kmsg.ConfigResourceType
}

func describeConfigRes(r kmsg.DescribeConfigsRequestResource) configRes {
	return configRes{name: r.ResourceName, typ: r.ResourceType}
}

func incrementalConfigRes(r kmsg.IncrementalAlterConfigsRequestResource) configRes {
	return configRes{name: r.ResourceName, typ: r.ResourceType}
}

func alterConfigRes(r kmsg.AlterConfigsRequestResource) configRes {
	return configRes{name: r.ResourceName, typ: r.ResourceType}
}

func configRule[T any](resources []T, conv func(T) configRes, topicAction, clusterAction string) rule {
	acc := make([]access, 0, len(resources))

	for _, res := range resources {
		c := conv(res)

		switch int8(c.typ) {
		case configResourceTopic:
			acc = append(acc, access{action: topicAction, kind: kindTopic, name: c.name})
		case configResourceBroker:
			acc = append(acc, access{action: clusterAction, kind: kindCluster})
		}
	}

	return rule{accesses: acc}
}

func ptrOrEmpty(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}

func slicesCloneStrings(in []string) []string {
	return append(make([]string, 0, len(in)), in...)
}
