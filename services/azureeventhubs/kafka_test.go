package azureeventhubs_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	"github.com/blackbirdworks/gopherstack/services/azureeventhubs"
)

func (e *env) kafkaAddr(t *testing.T) string {
	t.Helper()

	ln := listenLocal(t)
	require.NoError(t, e.h.ServeKafka(t.Context(), ln))

	return ln.Addr().String()
}

func (e *env) kafkaClient(t *testing.T, addr, user, pass string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()

	opts = append([]kgo.Opt{
		kgo.SeedBrokers(addr),
		kgo.DialTLSConfig(&tls.Config{RootCAs: e.pool, ServerName: "localhost", MinVersion: tls.VersionTLS12}),
		kgo.SASL(plain.Auth{User: user, Pass: pass}.AsMechanism()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	}, opts...)

	c, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	t.Cleanup(c.Close)

	return c
}

func (e *env) connString(key string) string {
	return fmt.Sprintf("Endpoint=sb://%s.servicebus.windows.net/;SharedAccessKeyName=%s;SharedAccessKey=%s",
		testNS, azureeventhubs.DefaultKeyName, key)
}

func (e *env) kafkaDefault(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()

	return e.kafkaClient(t, e.kafkaAddr(t), "$ConnectionString", e.connString(azureeventhubs.DefaultKeyValue), opts...)
}

func fetchAll(t *testing.T, c *kgo.Client, want int) []*kgo.Record {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), sdkTimeout)
	defer cancel()

	var out []*kgo.Record

	for len(out) < want {
		fs := c.PollFetches(ctx)
		require.NoError(t, fs.Err())
		fs.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}

	return out
}

func TestKafkaProduceConsume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []kgo.Opt
	}{
		{name: "idempotent"},
		{name: "non_idempotent", opts: []kgo.Opt{kgo.DisableIdempotentWrite()}},
		{name: "gzip", opts: []kgo.Opt{
			kgo.DisableIdempotentWrite(), kgo.ProducerBatchCompression(kgo.GzipCompression()),
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			prod := e.kafkaDefault(t, tt.opts...)

			recs := []*kgo.Record{
				{Topic: testHub, Partition: 1, Key: []byte("k1"), Value: []byte("v1"),
					Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("hv")}}},
				{Topic: testHub, Partition: 1, Key: []byte("k2"), Value: []byte("v2")},
				{Topic: testHub, Partition: 2, Value: []byte("v3")},
			}
			require.NoError(t, prod.ProduceSync(t.Context(), recs...).FirstErr())

			cons := e.kafkaDefault(t,
				kgo.ConsumePartitions(allPartitionsFromStart(3)))
			got := fetchAll(t, cons, 3)

			byValue := map[string]*kgo.Record{}
			for _, r := range got {
				byValue[string(r.Value)] = r
			}

			assert.Equal(t, "k1", string(byValue["v1"].Key))
			assert.Equal(t, int64(0), byValue["v1"].Offset)
			assert.Equal(t, int64(1), byValue["v2"].Offset)
			assert.Equal(t, int32(2), byValue["v3"].Partition)
			require.Len(t, byValue["v1"].Headers, 1)
			assert.Equal(t, "hv", string(byValue["v1"].Headers[0].Value))
		})
	}
}

func TestKafkaAMQPInterop(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)

	sendBatch(t, e.producer(t, azureeventhubs.DefaultKeyValue), nil, "from-amqp")

	cons := e.kafkaDefault(t, kgo.ConsumePartitions(allPartitionsFromStart(4)))
	got := fetchAll(t, cons, 1)
	assert.Equal(t, "from-amqp", string(got[0].Value))

	prod := e.kafkaDefault(t, kgo.DisableIdempotentWrite())
	require.NoError(t, prod.ProduceSync(t.Context(),
		&kgo.Record{Topic: testHub, Partition: 3, Value: []byte("from-kafka")}).FirstErr())

	info, err := e.b.PartitionInfo(azureeventhubs.HubRef{Namespace: testNS, Hub: testHub}, 3)
	require.NoError(t, err)
	assert.False(t, info.Empty)
	assert.Equal(t, int64(0), info.LastSequence)
}

func TestKafkaConsumerGroup(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	prod := e.kafkaDefault(t, kgo.DisableIdempotentWrite())

	require.NoError(t, prod.ProduceSync(t.Context(),
		&kgo.Record{Topic: testHub, Partition: 0, Value: []byte("a")},
		&kgo.Record{Topic: testHub, Partition: 0, Value: []byte("b")}).FirstErr())

	addr := e.kafkaAddr(t)
	user, pass := "$ConnectionString", e.connString(azureeventhubs.DefaultKeyValue)
	group := []kgo.Opt{kgo.ConsumerGroup("g1"), kgo.ConsumeTopics(testHub), kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())}

	c1 := e.kafkaClient(t, addr, user, pass, group...)
	got := fetchAll(t, c1, 2)
	assert.Equal(t, []string{"a", "b"}, []string{string(got[0].Value), string(got[1].Value)})
	require.NoError(t, c1.CommitRecords(t.Context(), got[0]))
	c1.Close()

	ref := azureeventhubs.HubRef{Namespace: testNS, Hub: testHub}
	off, err := e.b.KafkaOffset(ref, "g1", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(1), off, "commit stores the next offset to read")

	other, err := e.b.KafkaOffset(ref, "g1", 1)
	require.NoError(t, err)
	assert.Equal(t, int64(-1), other)

	c2 := e.kafkaClient(t, addr, user, pass, group...)
	resumed := fetchAll(t, c2, 1)
	assert.Equal(t, "b", string(resumed[0].Value))
}

func TestKafkaOffsetsSurviveSnapshot(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ref := azureeventhubs.HubRef{Namespace: testNS, Hub: testHub}
	require.NoError(t, e.b.CommitKafkaOffset(ref, "g", 2, 41))

	b2 := azureeventhubs.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), e.b.Snapshot(t.Context())))

	off, err := b2.KafkaOffset(ref, "g", 2)
	require.NoError(t, err)
	assert.Equal(t, int64(41), off)
}

func TestKafkaSASL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		user     string
		pass     string
		validate bool
		wantCode int16
	}{
		{name: "valid_key", validate: true, user: "$ConnectionString", pass: azureeventhubs.DefaultKeyValue},
		{
			name:     "rule_name_and_key",
			validate: true,
			user:     azureeventhubs.DefaultKeyName,
			pass:     azureeventhubs.DefaultKeyValue,
		},
		{name: "wrong_key", validate: true, user: "$ConnectionString", pass: "wrong", wantCode: 58},
		{name: "unvalidated", validate: false, user: "$ConnectionString", pass: "anything"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, tt.validate)
			pass := tt.pass
			if tt.user == "$ConnectionString" {
				pass = e.connString(tt.pass)
			}

			conn, err := tls.Dial("tcp", e.kafkaAddr(t), &tls.Config{
				RootCAs: e.pool, ServerName: "localhost", MinVersion: tls.VersionTLS12,
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })

			hs := kmsg.NewPtrSASLHandshakeRequest()
			hs.Version, hs.Mechanism = 1, "PLAIN"

			hsResp, ok := rawRoundTrip(t, conn, hs).(*kmsg.SASLHandshakeResponse)
			require.True(t, ok)
			require.Equal(t, int16(0), hsResp.ErrorCode)

			auth := kmsg.NewPtrSASLAuthenticateRequest()
			auth.SASLAuthBytes = []byte("\x00" + tt.user + "\x00" + pass)

			resp, ok := rawRoundTrip(t, conn, auth).(*kmsg.SASLAuthenticateResponse)
			require.True(t, ok)
			assert.Equal(t, tt.wantCode, resp.ErrorCode)
		})
	}
}

// rawRoundTrip sends one request on conn and decodes its response.
func rawRoundTrip(t *testing.T, conn net.Conn, req kmsg.Request) kmsg.Response {
	t.Helper()

	payload := binary.BigEndian.AppendUint16(nil, uint16(req.Key()))
	payload = binary.BigEndian.AppendUint16(payload, uint16(req.GetVersion()))
	payload = binary.BigEndian.AppendUint32(payload, 7)
	payload = binary.BigEndian.AppendUint16(payload, 0xffff)

	if req.IsFlexible() {
		payload = append(payload, 0)
	}

	payload = req.AppendTo(payload)
	_, err := conn.Write(binary.BigEndian.AppendUint32(nil, uint32(len(payload))))
	require.NoError(t, err)
	_, err = conn.Write(payload)
	require.NoError(t, err)

	var size [4]byte

	_, err = io.ReadFull(conn, size[:])
	require.NoError(t, err)

	body := make([]byte, binary.BigEndian.Uint32(size[:]))
	_, err = io.ReadFull(conn, body)
	require.NoError(t, err)

	resp := req.ResponseKind()
	resp.SetVersion(req.GetVersion())

	body = body[4:]
	if resp.IsFlexible() {
		body = body[1:]
	}

	require.NoError(t, resp.ReadFrom(body))

	return resp
}

func TestKafkaRights(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)

	for _, r := range []struct {
		name   string
		rights []string
	}{{"sendonly", []string{azureeventhubs.RightSend}}, {"listenonly", []string{azureeventhubs.RightListen}}} {
		rule, err := e.b.PutRule(testNS, "", r.name, r.rights)
		require.NoError(t, err)

		addr := e.kafkaAddr(t)
		c := e.kafkaClient(t, addr, r.name, rule.PrimaryKey, kgo.DisableIdempotentWrite())

		err = c.ProduceSync(t.Context(), &kgo.Record{Topic: testHub, Partition: 0, Value: []byte("x")}).FirstErr()
		if r.name == "sendonly" {
			require.NoError(t, err)

			continue
		}

		require.ErrorIs(t, err, kerr.TopicAuthorizationFailed)
	}
}

func TestKafkaUnknownTopic(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	c := e.kafkaDefault(t, kgo.DisableIdempotentWrite(), kgo.UnknownTopicRetries(0))

	err := c.ProduceSync(t.Context(), &kgo.Record{Topic: "missing", Partition: 0, Value: []byte("x")}).FirstErr()
	require.Error(t, err)
}

func TestKafkaCreateDeleteTopics(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	c := e.kafkaDefault(t)

	req := kmsg.NewPtrCreateTopicsRequest()
	req.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: "made", NumPartitions: 3, ReplicationFactor: 1}}

	resp, err := req.RequestWith(t.Context(), c)
	require.NoError(t, err)
	require.Len(t, resp.Topics, 1)
	assert.Equal(t, int16(0), resp.Topics[0].ErrorCode)

	info, err := e.b.Hub(azureeventhubs.HubRef{Namespace: testNS, Hub: "made"})
	require.NoError(t, err)
	assert.Len(t, info.PartitionIDs, 3)

	again, err := req.RequestWith(t.Context(), c)
	require.NoError(t, err)
	assert.Equal(t, kerr.TopicAlreadyExists.Code, again.Topics[0].ErrorCode)

	del := kmsg.NewPtrDeleteTopicsRequest()
	del.TopicNames = []string{"made"}
	delResp, err := del.RequestWith(t.Context(), c)
	require.NoError(t, err)
	assert.Equal(t, int16(0), delResp.Topics[0].ErrorCode)

	_, err = e.b.Hub(azureeventhubs.HubRef{Namespace: testNS, Hub: "made"})
	require.Error(t, err)
}

func TestKafkaListOffsets(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	c := e.kafkaDefault(t, kgo.DisableIdempotentWrite())
	require.NoError(t, c.ProduceSync(t.Context(),
		&kgo.Record{Topic: testHub, Partition: 0, Value: []byte("a")},
		&kgo.Record{Topic: testHub, Partition: 0, Value: []byte("b")}).FirstErr())

	adm := kmsg.NewPtrListOffsetsRequest()
	adm.Version = 5
	adm.ReplicaID = -1
	adm.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: testHub, Partitions: []kmsg.ListOffsetsRequestTopicPartition{
		{Partition: 0, Timestamp: -2}, {Partition: 0, Timestamp: -1},
	}}}

	resp, err := adm.RequestWith(t.Context(), c)
	require.NoError(t, err)

	ps := resp.Topics[0].Partitions
	assert.Equal(t, int64(0), ps[0].Offset)
	assert.Equal(t, int64(2), ps[1].Offset)
}

func TestKafkaGroupCoordinator(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := start
	h := azureeventhubs.NewGroupHarness(func() time.Time { return now })

	first := h.Join(t.Context(), "g", "", 4)
	assert.Equal(t, kerr.MemberIDRequired.Code, first.ErrorCode)
	require.NotEmpty(t, first.MemberID)

	joined := h.Join(t.Context(), "g", first.MemberID, 4)
	require.Equal(t, int16(0), joined.ErrorCode)
	assert.Equal(t, int32(1), joined.Generation)
	assert.Equal(t, first.MemberID, joined.LeaderID)
	assert.Equal(t, int16(0), h.Heartbeat("g", first.MemberID, joined.Generation))
	assert.Equal(t, kerr.IllegalGeneration.Code, h.Heartbeat("g", first.MemberID, 9))
	assert.Equal(t, kerr.UnknownMemberID.Code, h.Heartbeat("g", "nobody", 1))

	now = start.Add(5 * time.Second)
	h.Sweep()
	assert.Len(t, h.Members("g"), 1, "within the session timeout")

	now = start.Add(time.Minute)
	h.Sweep()
	assert.Empty(t, h.Members("g"), "session expired")
	assert.Equal(t, kerr.UnknownMemberID.Code, h.Heartbeat("g", first.MemberID, joined.Generation))
}

func TestKafkaClientRebalance(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	addr := e.kafkaAddr(t)
	user, pass := "$ConnectionString", e.connString(azureeventhubs.DefaultKeyValue)
	opts := make([]kgo.Opt, 0, 3)
	opts = append(opts, kgo.ConsumerGroup("rb"), kgo.ConsumeTopics(testHub))

	var assigned [2]chan map[string][]int32

	for i := range assigned {
		assigned[i] = make(chan map[string][]int32, 8)

		c := e.kafkaClient(t, addr, user, pass,
			append(opts, kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
				assigned[i] <- m
			}))...)
		ctx, cancel := context.WithTimeout(t.Context(), sdkTimeout)

		go func() {
			defer cancel()

			for ctx.Err() == nil {
				c.PollFetches(ctx)
			}
		}()
	}

	total := 0

	for _, ch := range assigned {
		select {
		case m := <-ch:
			total += len(m[testHub])
		case <-t.Context().Done():
			t.Fatal("no assignment")
		}
	}

	assert.Positive(t, total)
}

func allPartitionsFromStart(n int) map[string]map[int32]kgo.Offset {
	m := map[int32]kgo.Offset{}
	for i := range n {
		m[int32(i)] = kgo.NewOffset().AtStart()
	}

	return map[string]map[int32]kgo.Offset{testHub: m}
}

func TestKafkaRuleScopedToNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		endpoint string
		wantCode int16
	}{
		{name: "own_namespace", endpoint: "other", wantCode: 0},
		{name: "foreign_namespace", endpoint: testNS, wantCode: 58},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			require.NoError(t, e.b.EnsureNamespace("other"))

			rule, err := e.b.PutRule("other", "", "r1", []string{azureeventhubs.RightSend})
			require.NoError(t, err)

			conn, err := tls.Dial("tcp", e.kafkaAddr(t), &tls.Config{
				RootCAs: e.pool, ServerName: "localhost", MinVersion: tls.VersionTLS12,
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })

			hs := kmsg.NewPtrSASLHandshakeRequest()
			hs.Version, hs.Mechanism = 1, "PLAIN"
			require.Equal(t, int16(0), rawRoundTrip(t, conn, hs).(*kmsg.SASLHandshakeResponse).ErrorCode)

			cs := fmt.Sprintf("Endpoint=sb://%s.servicebus.windows.net/;SharedAccessKeyName=r1;SharedAccessKey=%s",
				tt.endpoint, rule.PrimaryKey)
			auth := kmsg.NewPtrSASLAuthenticateRequest()
			auth.SASLAuthBytes = []byte("\x00$ConnectionString\x00" + cs)

			resp, ok := rawRoundTrip(t, conn, auth).(*kmsg.SASLAuthenticateResponse)
			require.True(t, ok)
			assert.Equal(t, tt.wantCode, resp.ErrorCode)
		})
	}
}

func gzipBatch(t *testing.T, inflated int) []byte {
	t.Helper()

	var zb bytes.Buffer

	zw := gzip.NewWriter(&zb)
	_, err := zw.Write(make([]byte, inflated))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	rb := kmsg.RecordBatch{
		PartitionLeaderEpoch: -1, Magic: 2, Attributes: 1, ProducerID: -1, ProducerEpoch: -1, FirstSequence: -1,
		Records: zb.Bytes(),
	}
	out := rb.AppendTo(nil)
	binary.BigEndian.PutUint32(out[8:12], uint32(len(out)-12))
	binary.BigEndian.PutUint32(out[17:21], crc32.Checksum(out[21:], crc32.MakeTable(crc32.Castagnoli)))

	return out
}

func TestKafkaInflateBudgetSharedAcrossBatches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		batches int
		wantErr bool
	}{
		{name: "one_batch", batches: 1},
		{name: "many_batches", batches: 3, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var payload []byte
			for range tt.batches {
				payload = append(payload, gzipBatch(t, 10<<20)...)
			}

			err := azureeventhubs.SplitBatchesError(payload)
			if tt.wantErr {
				require.ErrorIs(t, err, azureeventhubs.ErrBatchTooLargeForTest)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestKafkaFrameLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		size    uint32
		authed  bool
		wantErr bool
	}{
		{name: "unauth_small", size: 100, authed: false},
		{name: "unauth_over", size: 1 << 20, authed: false, wantErr: true},
		{name: "auth_large", size: 1 << 20, authed: true},
		{name: "auth_over", size: 1 << 30, authed: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			frame := binary.BigEndian.AppendUint32(nil, tt.size)
			if !tt.wantErr {
				frame = append(frame, make([]byte, tt.size)...)
			}

			got, err := azureeventhubs.ReadFrameForTest(bytes.NewReader(frame), tt.authed)
			if tt.wantErr {
				require.ErrorIs(t, err, azureeventhubs.ErrKafkaFrameForTest)

				return
			}

			require.NoError(t, err)
			assert.Len(t, got, int(tt.size))
		})
	}
}
