package kafka

import (
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const gateClusterArn = "arn:aws:kafka:us-east-1:123456789012:cluster/demo/abcd-1"

func allowOnly(grants ...string) iamAuthorizer {
	set := make(map[string]bool, len(grants))
	for _, g := range grants {
		set[g] = true
	}

	return func(action, resource string) bool { return set[action+"|"+resource] }
}

type gateHarness struct {
	client   net.Conn
	upstream net.Conn
	done     chan struct{}
}

func startGate(t *testing.T, authz iamAuthorizer) *gateHarness {
	t.Helper()

	clientEnd, gateClient := net.Pipe()
	gateUp, upstreamEnd := net.Pipe()
	g := newGate(gateClient, gateUp, authz, gateClusterArn)
	h := &gateHarness{client: clientEnd, upstream: upstreamEnd, done: make(chan struct{})}

	go func() {
		defer close(h.done)

		g.run()
	}()

	t.Cleanup(func() {
		_ = clientEnd.Close()
		_ = upstreamEnd.Close()
		<-h.done
	})

	return h
}

func (h *gateHarness) send(t *testing.T, corr uint32, req kmsg.Request) {
	t.Helper()
	require.NoError(t, writeRequest(h.client, corr, req))
}

// echoUpstream answers every upstream request with its own header minus key and version (corr first).
func (h *gateHarness) echoUpstream() {
	go func() {
		for {
			frame, err := readFrame(h.upstream)
			if err != nil || writeFrame(h.upstream, frame[4:]) != nil {
				return
			}
		}
	}()
}

func (h *gateHarness) recv(t *testing.T) []byte {
	t.Helper()

	frame, err := readFrame(h.client)
	require.NoError(t, err)

	return frame
}

func corrOf(frame []byte) uint32 { return binary.BigEndian.Uint32(frame[:4]) }

func gateTopicARN(name string) string {
	return "arn:aws:kafka:us-east-1:123456789012:topic/demo/abcd-1/" + name
}

func gateGroupARN(name string) string {
	return "arn:aws:kafka:us-east-1:123456789012:group/demo/abcd-1/" + name
}

func TestGate_DeniedRequestsGetProtocolErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req  kmsg.Request
		resp kmsg.Response
		name string
		code int16
	}{
		{name: "produce", code: errCodeTopicAuth, req: produceReq("t1", 1), resp: &kmsg.ProduceResponse{}},
		{name: "fetch", code: errCodeTopicAuth, req: fetchReq("t1"), resp: &kmsg.FetchResponse{}},
		{name: "metadata", code: errCodeTopicAuth, req: metadataReq("t1"), resp: &kmsg.MetadataResponse{}},
		{name: "create_topics", code: errCodeTopicAuth, req: createTopicsReq("t1"), resp: &kmsg.CreateTopicsResponse{}},
		{name: "delete_topics", code: errCodeTopicAuth, req: deleteTopicsReq("t1"), resp: &kmsg.DeleteTopicsResponse{}},
		{name: "join_group", code: errCodeGroupAuth, req: joinReq("g1"), resp: &kmsg.JoinGroupResponse{}},
		{name: "init_producer", code: errCodeClusterAuth, req: initProducerReq(), resp: &kmsg.InitProducerIDResponse{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := startGate(t, allowOnly())
			h.send(t, 7, tt.req)

			frame := h.recv(t)
			assert.Equal(t, uint32(7), corrOf(frame))

			resp := tt.resp
			resp.SetVersion(tt.req.GetVersion())

			off := 4
			if resp.IsFlexible() {
				off++
			}

			require.NoError(t, resp.ReadFrom(frame[off:]))
			assert.Equal(t, tt.code, firstErrorCode(resp))
		})
	}
}

func firstErrorCode(resp kmsg.Response) int16 {
	switch r := resp.(type) {
	case *kmsg.ProduceResponse:
		return r.Topics[0].Partitions[0].ErrorCode
	case *kmsg.FetchResponse:
		return r.Topics[0].Partitions[0].ErrorCode
	case *kmsg.MetadataResponse:
		return r.Topics[0].ErrorCode
	case *kmsg.CreateTopicsResponse:
		return r.Topics[0].ErrorCode
	case *kmsg.DeleteTopicsResponse:
		return r.Topics[0].ErrorCode
	case *kmsg.JoinGroupResponse:
		return r.ErrorCode
	case *kmsg.InitProducerIDResponse:
		return r.ErrorCode
	}

	return 0
}

func TestGate_AllowedRequestsReachUpstream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req    kmsg.Request
		name   string
		grants []string
	}{
		{name: "produce", req: produceReq("t1", 1), grants: []string{"kafka-cluster:WriteData|" + gateTopicARN("t1")}},
		{name: "fetch", req: fetchReq("t1"), grants: []string{"kafka-cluster:ReadData|" + gateTopicARN("t1")}},
		{name: "join", req: joinReq("g1"), grants: []string{"kafka-cluster:AlterGroup|" + gateGroupARN("g1")}},
		{
			name: "idempotent", req: initProducerReq(),
			grants: []string{"kafka-cluster:WriteDataIdempotently|" + gateClusterArn},
		},
		{name: "ungated_api", req: kmsg.NewPtrFindCoordinatorRequest()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := startGate(t, allowOnly(tt.grants...))
			h.echoUpstream()
			h.send(t, 9, tt.req)

			frame := h.recv(t)
			assert.Equal(t, uint32(9), corrOf(frame))
		})
	}
}

func TestGate_LocalReplyKeepsResponseOrder(t *testing.T) {
	t.Parallel()

	h := startGate(t, allowOnly("kafka-cluster:ReadData|"+gateTopicARN("ok")))

	upstreamSaw := make(chan []byte, 1)

	go func() {
		frame, err := readFrame(h.upstream)
		if err != nil {
			return
		}

		upstreamSaw <- frame
	}()

	h.send(t, 1, fetchReq("ok"))
	h.send(t, 2, produceReq("nope", 1))

	frame := <-upstreamSaw
	require.NoError(t, writeFrame(h.upstream, frame[4:]))

	assert.Equal(t, uint32(1), corrOf(h.recv(t)))
	assert.Equal(t, uint32(2), corrOf(h.recv(t)))
}

func TestGate_SilentProduceDeniedSendsNothing(t *testing.T) {
	t.Parallel()

	h := startGate(t, allowOnly())

	h.send(t, 1, produceReq("nope", 0))
	h.send(t, 2, metadataReq("nope"))

	assert.Equal(t, uint32(2), corrOf(h.recv(t)))
}

func TestGate_UnansweredDenialClosesConnection(t *testing.T) {
	t.Parallel()

	h := startGate(t, allowOnly())

	alter := kmsg.NewPtrAlterConfigsRequest()
	alter.SetVersion(1)

	res := kmsg.NewAlterConfigsRequestResource()
	res.ResourceType, res.ResourceName = kmsg.ConfigResourceTypeBroker, "1"
	alter.Resources = append(alter.Resources, res)

	h.send(t, 1, alter)

	_, err := readFrame(h.client)
	require.ErrorIs(t, err, io.EOF)
}

func TestCapVersions(t *testing.T) {
	t.Parallel()

	av := kmsg.NewPtrApiVersionsResponse()
	av.SetVersion(3)

	for _, k := range []int16{produceAPIKey, fetchAPIKey, offsetCommitAPIKey, 3} {
		e := kmsg.NewApiVersionsResponseApiKey()
		e.ApiKey, e.MaxVersion = k, 20
		av.ApiKeys = append(av.ApiKeys, e)
	}

	raw := binary.BigEndian.AppendUint32(nil, 5)
	got := kmsg.NewPtrApiVersionsResponse()
	got.SetVersion(3)
	require.NoError(t, got.ReadFrom(capVersions(av.AppendTo(raw), 3)[4:]))

	want := map[int16]int16{produceAPIKey: 12, fetchAPIKey: 12, offsetCommitAPIKey: 9, 3: 20}
	for _, k := range got.ApiKeys {
		assert.Equal(t, want[k.ApiKey], k.MaxVersion, "api key %d", k.ApiKey)
	}
}

func TestResourceArn(t *testing.T) {
	t.Parallel()

	assert.Equal(t, gateTopicARN("a.b"), resourceArn(gateClusterArn, kindTopic, "a.b"))
	assert.Equal(t, gateGroupARN("g"), resourceArn(gateClusterArn, kindGroup, "g"))
	assert.Equal(t, gateClusterArn, resourceArn(gateClusterArn, kindCluster, ""))
}

func produceReq(topic string, acks int16) kmsg.Request {
	r := kmsg.NewPtrProduceRequest()
	r.SetVersion(9)
	r.Acks = acks

	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = topic
	rt.Partitions = []kmsg.ProduceRequestTopicPartition{kmsg.NewProduceRequestTopicPartition()}
	r.Topics = append(r.Topics, rt)

	return r
}

func fetchReq(topic string) kmsg.Request {
	r := kmsg.NewPtrFetchRequest()
	r.SetVersion(12)

	rt := kmsg.NewFetchRequestTopic()
	rt.Topic = topic
	rt.Partitions = []kmsg.FetchRequestTopicPartition{kmsg.NewFetchRequestTopicPartition()}
	r.Topics = append(r.Topics, rt)

	return r
}

func metadataReq(topic string) kmsg.Request {
	r := kmsg.NewPtrMetadataRequest()
	r.SetVersion(9)

	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = &topic
	r.Topics = append(r.Topics, rt)

	return r
}

func createTopicsReq(topic string) kmsg.Request {
	r := kmsg.NewPtrCreateTopicsRequest()
	r.SetVersion(5)

	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = topic
	r.Topics = append(r.Topics, rt)

	return r
}

func deleteTopicsReq(topic string) kmsg.Request {
	r := kmsg.NewPtrDeleteTopicsRequest()
	r.SetVersion(5)
	r.TopicNames = []string{topic}

	return r
}

func joinReq(group string) kmsg.Request {
	r := kmsg.NewPtrJoinGroupRequest()
	r.SetVersion(7)
	r.Group = group

	return r
}

func initProducerReq() kmsg.Request {
	r := kmsg.NewPtrInitProducerIDRequest()
	r.SetVersion(4)

	return r
}
