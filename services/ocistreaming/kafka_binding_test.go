package ocistreaming_test

import (
	"encoding/binary"
	"io"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/streaming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"

	oci "github.com/blackbirdworks/gopherstack/services/ocistreaming"
)

func (k *kafkaEnv) mkPoolStream(t *testing.T, poolID, name string) streaming.Stream {
	t.Helper()

	req := streaming.CreateStreamRequest{}
	req.Name, req.Partitions, req.StreamPoolId = new(name), new(1), new(poolID)

	resp, err := k.admin.CreateStream(t.Context(), req)
	require.NoError(t, err)
	k.settle()

	got, err := k.admin.GetStream(t.Context(), streaming.GetStreamRequest{StreamId: resp.Id})
	require.NoError(t, err)

	return got.Stream
}

func rawCall[R kmsg.Response](t *testing.T, c net.Conn, corr int32, req kmsg.Request, resp R) R {
	t.Helper()

	resp.SetVersion(req.GetVersion())
	require.NoError(t, resp.ReadFrom(rawSend(t, c, req.Key(), req.GetVersion(), corr, req)))

	return resp
}

type twoPools struct {
	*kafkaEnv
	connA, connB net.Conn
	a, b         streaming.Stream
	poolA, poolB string
}

func newTwoPools(t *testing.T) *twoPools {
	t.Helper()

	k := newKafkaEnv(t, nil)
	pa := k.mkPool(t, "pa", nil)
	pb := k.mkPool(t, "pb", nil)
	tp := &twoPools{kafkaEnv: k, poolA: *pa.Id, poolB: *pb.Id}
	tp.a = k.mkPoolStream(t, tp.poolA, "shared")
	tp.b = k.mkPoolStream(t, tp.poolB, "shared")
	k.mkPoolStream(t, tp.poolA, "a-only")
	k.mkPoolStream(t, tp.poolB, "b-only")

	ca, code := rawAuth(t, k, testTenancy+"/alice/"+tp.poolA, "tok")
	require.Equal(t, int16(0), code)

	cb, code := rawAuth(t, k, testTenancy+"/alice/"+tp.poolB, "tok")
	require.Equal(t, int16(0), code)

	tp.connA, tp.connB = ca, cb

	return tp
}

func TestKafkaPoolBindingEveryAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, tp *twoPools)
		name string
	}{
		{name: "metadata lists only own topics", run: func(t *testing.T, tp *twoPools) {
			t.Helper()

			md := kmsg.NewPtrMetadataRequest()
			md.Version = 1
			md.Topics = nil

			got := rawCall(t, tp.connA, 1, md, kmsg.NewPtrMetadataResponse())

			names := make([]string, 0, len(got.Topics))
			for _, tt := range got.Topics {
				names = append(names, *tt.Topic)
			}

			slices.Sort(names)
			assert.Equal(t, []string{"a-only", "shared"}, names)
		}},
		{name: "create topic lands in own pool", run: func(t *testing.T, tp *twoPools) {
			t.Helper()

			req := kmsg.NewPtrCreateTopicsRequest()
			req.Version = 2
			topic := kmsg.NewCreateTopicsRequestTopic()
			topic.Topic, topic.NumPartitions, topic.ReplicationFactor = "made", 1, 1
			req.Topics = []kmsg.CreateTopicsRequestTopic{topic}

			got := rawCall(t, tp.connA, 1, req, kmsg.NewPtrCreateTopicsResponse())
			require.Equal(t, int16(0), got.Topics[0].ErrorCode)

			list, err := tp.admin.ListStreams(t.Context(), streamingListReq(tp.poolB))
			require.NoError(t, err)

			for _, s := range list.Items {
				assert.NotEqual(t, "made", *s.Name)
			}
		}},
		{name: "delete topic leaves other pool", run: func(t *testing.T, tp *twoPools) {
			t.Helper()

			req := kmsg.NewPtrDeleteTopicsRequest()
			req.Version = 1
			req.TopicNames = []string{"shared", "b-only"}

			got := rawCall(t, tp.connA, 1, req, kmsg.NewPtrDeleteTopicsResponse())
			assert.Equal(t, int16(0), got.Topics[0].ErrorCode)
			assert.Equal(t, int16(3), got.Topics[1].ErrorCode, "b-only is not in pool A")

			gb, err := tp.admin.GetStream(t.Context(), streaming.GetStreamRequest{StreamId: tp.b.Id})
			require.NoError(t, err)
			assert.Equal(t, streaming.StreamLifecycleStateActive, gb.LifecycleState)
		}},
		{name: "offsets are per pool", run: func(t *testing.T, tp *twoPools) {
			t.Helper()

			cm := kmsg.NewPtrOffsetCommitRequest()
			cm.Version, cm.Group, cm.Generation = 2, "g", -1
			cm.Topics = []kmsg.OffsetCommitRequestTopic{{
				Topic: "shared", Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: 0}},
			}}

			got := rawCall(t, tp.connA, 1, cm, kmsg.NewPtrOffsetCommitResponse())
			require.Equal(t, int16(0), got.Topics[0].Partitions[0].ErrorCode)

			fetch := func(c net.Conn) int64 {
				of := kmsg.NewPtrOffsetFetchRequest()
				of.Version, of.Group = 1, "g"
				of.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: "shared", Partitions: []int32{0}}}

				return rawCall(t, c, 2, of, kmsg.NewPtrOffsetFetchResponse()).Topics[0].Partitions[0].Offset
			}

			assert.Equal(t, int64(0), fetch(tp.connA))
			assert.Equal(t, int64(-1), fetch(tp.connB), "pool B never committed")
		}},
		{name: "group ids do not collide", run: func(t *testing.T, tp *twoPools) {
			t.Helper()

			join := func(c net.Conn) *kmsg.JoinGroupResponse {
				jr := kmsg.NewPtrJoinGroupRequest()
				jr.Version, jr.Group, jr.ProtocolType = 3, "g", "consumer"
				jr.SessionTimeoutMillis, jr.RebalanceTimeoutMillis = 10000, 10000
				jr.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: []byte{1}}}

				return rawCall(t, c, 1, jr, kmsg.NewPtrJoinGroupResponse())
			}

			ra, rb := join(tp.connA), join(tp.connB)
			assert.Equal(t, int16(0), ra.ErrorCode)
			assert.Equal(t, int16(0), rb.ErrorCode)
			assert.Equal(t, ra.MemberID, ra.LeaderID)
			assert.Equal(t, rb.MemberID, rb.LeaderID, "each pool elects its own leader")
			assert.Len(t, ra.Members, 1)
			assert.Len(t, rb.Members, 1)

			hb := kmsg.NewPtrHeartbeatRequest()
			hb.Group, hb.MemberID, hb.Generation = "g", ra.MemberID, ra.Generation
			assert.Equal(t, int16(25), rawCall(t, tp.connB, 2, hb, kmsg.NewPtrHeartbeatResponse()).ErrorCode,
				"a member of pool A is unknown to pool B")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newTwoPools(t))
		})
	}
}

func TestKafkaAuthUserForms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		user     func(pool string) string
		name     string
		wantCode int16
	}{
		{name: "three segments", user: func(p string) string { return "ten/alice/" + p }},
		{name: "four segments", user: func(p string) string { return "ten/dom/alice/" + p }},
		{name: "five segments", user: func(p string) string { return "ten/dom/x/alice/" + p }, wantCode: 58},
		{name: "empty tenancy", user: func(p string) string { return "/alice/" + p }, wantCode: 58},
		{name: "empty domain", user: func(p string) string { return "ten//alice/" + p }, wantCode: 58},
		{name: "empty user", user: func(p string) string { return "ten//" + p }, wantCode: 58},
		{name: "empty pool", user: func(string) string { return "ten/dom/alice/" }, wantCode: 58},
		{name: "two segments", user: func(p string) string { return "alice/" + p }, wantCode: 58},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			k := newKafkaEnv(t, func(user, token string) bool { return user == "alice" && token == "good" })
			s := k.mkStream(t, "t", 1)

			_, code := rawAuth(t, k, tt.user(*s.StreamPoolId), "good")
			assert.Equal(t, tt.wantCode, code)
		})
	}
}

func TestKafkaVerifierRunsForUnknownPool(t *testing.T) {
	t.Parallel()

	var calls int

	k := newKafkaEnv(t, func(string, string) bool {
		calls++

		return true
	})

	_, code := rawAuth(t, k, "ten/alice/ocid1.streampool.oc1..nope", "tok")
	assert.Equal(t, int16(58), code)
	assert.Equal(t, 1, calls, "the token check does the same work whether or not the pool exists")
}

func TestKafkaAcksZeroSendsNoResponse(t *testing.T) {
	t.Parallel()

	k := newKafkaEnv(t, nil)
	s := k.mkStream(t, "t", 1)

	c, code := rawAuth(t, k, testTenancy+"/alice/"+*s.StreamPoolId, "tok")
	require.Equal(t, int16(0), code)

	pr := kmsg.NewPtrProduceRequest()
	pr.Version, pr.Acks, pr.TimeoutMillis = 3, 0, 1000
	pr.Topics = []kmsg.ProduceRequestTopic{{Topic: "t", Partitions: []kmsg.ProduceRequestTopicPartition{
		{Records: oci.EncodeTestBatch([]byte("x"))},
	}}}

	hdr := binary.BigEndian.AppendUint16(nil, 0)
	hdr = binary.BigEndian.AppendUint16(hdr, 3)
	hdr = binary.BigEndian.AppendUint32(hdr, 41)
	hdr = binary.BigEndian.AppendUint16(hdr, 0xffff)
	frame := slices.Concat(hdr, pr.AppendTo(nil))
	_, err := c.Write(append(binary.BigEndian.AppendUint32(nil, uint32(len(frame))), frame...))
	require.NoError(t, err)

	av := kmsg.NewPtrApiVersionsRequest()
	av.Version = 0
	require.NoError(t, rawCallCorr(t, c, 42, av))

	rng, rerr := k.backend.PartitionRanges(*s.Id)
	require.NoError(t, rerr)
	assert.Equal(t, int64(1), rng[0].End, "the fire-and-forget produce still lands")
}

// rawCallCorr sends req and requires the very next frame to carry corr.
func rawCallCorr(t *testing.T, c net.Conn, corr int32, req kmsg.Request) error {
	t.Helper()

	hdr := binary.BigEndian.AppendUint16(nil, uint16(req.Key()))
	hdr = binary.BigEndian.AppendUint16(hdr, uint16(req.GetVersion()))
	hdr = binary.BigEndian.AppendUint32(hdr, uint32(corr))
	hdr = binary.BigEndian.AppendUint16(hdr, 0xffff)
	frame := slices.Concat(hdr, req.AppendTo(nil))

	_, err := c.Write(append(binary.BigEndian.AppendUint32(nil, uint32(len(frame))), frame...))
	require.NoError(t, err)
	require.NoError(t, c.SetReadDeadline(time.Now().Add(10*time.Second)))

	var sz [8]byte
	_, err = io.ReadFull(c, sz[:])
	require.NoError(t, err)
	assert.Equal(t, corr, int32(binary.BigEndian.Uint32(sz[4:])), "first response frame belongs to the next request")

	rest := make([]byte, binary.BigEndian.Uint32(sz[:4])-4)
	_, err = io.ReadFull(c, rest)

	return err
}
