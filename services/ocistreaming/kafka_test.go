package ocistreaming_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	"github.com/oracle/oci-go-sdk/v65/streaming"

	oci "github.com/blackbirdworks/gopherstack/services/ocistreaming"
)

type kafkaEnv struct {
	*env
	pool *x509.CertPool
	addr string
}

func newKafkaEnv(t *testing.T, verify func(user, token string) bool) *kafkaEnv {
	t.Helper()

	e := newEnv(t, envOpts{})

	if verify != nil {
		e.backend.SetTokenVerifier(verify)
	}

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, e.handler.ServeKafka(t.Context(), ln))

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(e.handler.KafkaCertificatePEM()))

	return &kafkaEnv{env: e, addr: ln.Addr().String(), pool: pool}
}

func (k *kafkaEnv) client(t *testing.T, poolID, token string, extra ...kgo.Opt) *kgo.Client {
	t.Helper()

	opts := []kgo.Opt{
		kgo.SeedBrokers(k.addr),
		kgo.DialTLSConfig(&tls.Config{RootCAs: k.pool, MinVersion: tls.VersionTLS12}),
		kgo.SASL(plain.Auth{User: testTenancy + "/dom/alice/" + poolID, Pass: token}.AsMechanism()),
		kgo.DisableIdempotentWrite(),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	}

	c, err := kgo.NewClient(append(opts, extra...)...)
	require.NoError(t, err)
	t.Cleanup(c.Close)

	return c
}

func TestKafkaProduceFetchRoundTrip(t *testing.T) {
	t.Parallel()

	k := newKafkaEnv(t, nil)
	s := k.mkStream(t, "orders", 2)
	cl := k.client(t, *s.StreamPoolId, "tok", kgo.ConsumeTopics("orders"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))

	res := cl.ProduceSync(t.Context(),
		&kgo.Record{Topic: "orders", Partition: 1, Key: []byte("k"), Value: []byte("hello")},
		&kgo.Record{Topic: "orders", Partition: 1, Value: []byte("world")})
	require.NoError(t, res.FirstErr())

	rng, err := k.backend.PartitionRanges(*s.Id)
	require.NoError(t, err)
	assert.Equal(t, int64(2), rng[1].End)
	assert.Equal(t, int64(0), rng[0].End)

	ctx, cancel := t.Context(), func() {}
	defer cancel()

	var got []string

	for len(got) < 2 {
		fetches := cl.PollFetches(ctx)
		require.NoError(t, fetches.Err())
		fetches.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}

	assert.Equal(t, []string{"hello", "world"}, got)
}

func rawSend(t *testing.T, c net.Conn, key, version int16, corr int32, req kmsg.Request) []byte {
	t.Helper()

	hdr := binary.BigEndian.AppendUint16(nil, uint16(key))
	hdr = binary.BigEndian.AppendUint16(hdr, uint16(version))
	hdr = binary.BigEndian.AppendUint32(hdr, uint32(corr))
	hdr = binary.BigEndian.AppendUint16(hdr, 0xffff)
	frame := slices.Concat(hdr, req.AppendTo(nil))

	_, err := c.Write(append(binary.BigEndian.AppendUint32(nil, uint32(len(frame))), frame...))
	require.NoError(t, err)
	require.NoError(t, c.SetReadDeadline(time.Now().Add(10*time.Second)))

	var sz [4]byte
	_, err = io.ReadFull(c, sz[:])
	require.NoError(t, err)

	out := make([]byte, binary.BigEndian.Uint32(sz[:]))
	_, err = io.ReadFull(c, out)
	require.NoError(t, err)

	return out[4:]
}

// rawAuth runs the SASL exchange and returns the SaslAuthenticate error code.
func rawAuth(t *testing.T, k *kafkaEnv, user, token string) (net.Conn, int16) {
	t.Helper()

	c := rawConn(t, k)

	hs := kmsg.NewPtrSASLHandshakeRequest()
	hs.Version, hs.Mechanism = 1, "PLAIN"
	hr := kmsg.NewPtrSASLHandshakeResponse()
	hr.Version = 1
	require.NoError(t, hr.ReadFrom(rawSend(t, c, 17, 1, 1, hs)))
	require.Equal(t, int16(0), hr.ErrorCode)

	au := kmsg.NewPtrSASLAuthenticateRequest()
	au.Version, au.SASLAuthBytes = 1, []byte("\x00"+user+"\x00"+token)
	ar := kmsg.NewPtrSASLAuthenticateResponse()
	ar.Version = 1
	require.NoError(t, ar.ReadFrom(rawSend(t, c, 36, 1, 2, au)))

	return c, ar.ErrorCode
}

func TestKafkaAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		user     func(poolID string) string
		token    string
		wantCode int16
	}{
		{name: "valid", user: func(p string) string { return testTenancy + "/alice/" + p }, token: "good"},
		{name: "bad token", user: func(p string) string { return testTenancy + "/alice/" + p },
			token: "bad", wantCode: 58},
		{name: "unknown user", user: func(p string) string { return testTenancy + "/mallory/" + p },
			token: "good", wantCode: 58},
		{name: "missing pool", user: func(string) string { return testTenancy + "/alice/ocid1.streampool.oc1..nope" },
			token: "good", wantCode: 58},
		{name: "short user", user: func(string) string { return "alice" }, token: "good", wantCode: 58},
		{
			name:     "empty token",
			user:     func(p string) string { return testTenancy + "/alice/" + p },
			token:    "",
			wantCode: 58,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			k := newKafkaEnv(t, func(user, token string) bool { return user == "alice" && token == "good" })
			s := k.mkStream(t, "t", 1)

			_, code := rawAuth(t, k, tt.user(*s.StreamPoolId), tt.token)
			assert.Equal(t, tt.wantCode, code)
		})
	}
}

func TestKafkaCredentialBoundToPool(t *testing.T) {
	t.Parallel()

	k := newKafkaEnv(t, nil)
	poolB := k.mkPool(t, "poolb", nil)
	a := k.mkStream(t, "shared", 1)

	c, code := rawAuth(t, k, testTenancy+"/alice/"+*poolB.Id, "tok")
	require.Equal(t, int16(0), code)

	topic := "shared"
	md := kmsg.NewPtrMetadataRequest()
	md.Version = 1
	md.Topics = []kmsg.MetadataRequestTopic{{Topic: &topic}}
	mr := kmsg.NewPtrMetadataResponse()
	mr.Version = 1
	require.NoError(t, mr.ReadFrom(rawSend(t, c, 3, 1, 3, md)))
	require.Len(t, mr.Topics, 1)
	assert.Equal(t, int16(3), mr.Topics[0].ErrorCode)

	pr := kmsg.NewPtrProduceRequest()
	pr.Version, pr.Acks, pr.TimeoutMillis = 3, 1, 1000
	pr.Topics = []kmsg.ProduceRequestTopic{{Topic: "shared", Partitions: []kmsg.ProduceRequestTopicPartition{
		{Records: oci.EncodeTestBatch([]byte("x"))},
	}}}
	pres := kmsg.NewPtrProduceResponse()
	pres.Version = 3
	require.NoError(t, pres.ReadFrom(rawSend(t, c, 0, 3, 4, pr)))
	assert.Equal(t, int16(3), pres.Topics[0].Partitions[0].ErrorCode)

	rng, rerr := k.backend.PartitionRanges(*a.Id)
	require.NoError(t, rerr)
	assert.Equal(t, int64(0), rng[0].End)
}

func TestKafkaEnforceAuthWithoutVerifierRejects(t *testing.T) {
	t.Parallel()

	k := newKafkaEnv(t, nil)
	s := k.mkStream(t, "t", 1)

	h2 := oci.NewHandler(k.backend)
	h2.EnforceAuth = true

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, h2.ServeKafka(t.Context(), ln))
	t.Cleanup(func() { h2.Shutdown(context.Background()) })

	k2 := &kafkaEnv{env: k.env, addr: ln.Addr().String(), pool: x509.NewCertPool()}
	require.True(t, k2.pool.AppendCertsFromPEM(h2.KafkaCertificatePEM()))

	_, code := rawAuth(t, k2, testTenancy+"/alice/"+*s.StreamPoolId, "tok")
	assert.Equal(t, int16(58), code)
}

func TestKafkaBootstrapServers(t *testing.T) {
	t.Parallel()

	k := newKafkaEnv(t, nil)
	p := k.mkPool(t, "bs", nil)

	got, err := k.admin.GetStreamPool(t.Context(), streamingGetPool(*p.Id))
	require.NoError(t, err)
	require.NotNil(t, got.KafkaSettings.BootstrapServers)

	_, port, splitErr := net.SplitHostPort(k.addr)
	require.NoError(t, splitErr)
	assert.Equal(t, "localhost:"+port, *got.KafkaSettings.BootstrapServers)
}

func rawConn(t *testing.T, k *kafkaEnv) *tls.Conn {
	t.Helper()

	c, err := tls.Dial("tcp", k.addr, &tls.Config{RootCAs: k.pool, MinVersion: tls.VersionTLS12})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func TestKafkaFrameLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		size uint32
	}{
		{name: "zero", size: 0},
		{name: "over preauth cap", size: 17 << 10},
		{name: "huge", size: 1 << 31},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			k := newKafkaEnv(t, nil)
			c := rawConn(t, k)

			require.NoError(t, binary.Write(c, binary.BigEndian, tt.size))
			require.NoError(t, c.SetReadDeadline(time.Now().Add(5*time.Second)))

			buf := make([]byte, 1)
			_, err := c.Read(buf)
			require.Error(t, err)

			if ne, ok := errors.AsType[net.Error](err); ok {
				assert.False(t, ne.Timeout(), "the server must close the connection, not wait for the body")
			}
		})
	}
}

func TestKafkaPostAuthFrameLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		size   uint32
		closed bool
	}{
		{name: "above pre-auth cap is allowed", size: 20 << 10},
		{name: "over frame cap", size: 8<<20 + 1, closed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			k := newKafkaEnv(t, nil)
			s := k.mkStream(t, "t", 1)

			c, code := rawAuth(t, k, testTenancy+"/alice/"+*s.StreamPoolId, "tok")
			require.Equal(t, int16(0), code)

			require.NoError(t, binary.Write(c, binary.BigEndian, tt.size))
			require.NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))

			_, err := c.Read(make([]byte, 1))
			require.Error(t, err)

			ne, ok := errors.AsType[net.Error](err)
			isTimeout := ok && ne.Timeout()
			assert.Equal(t, !tt.closed, isTimeout)
		})
	}
}

func TestKafkaCreateTopicPoolDefaults(t *testing.T) {
	t.Parallel()

	k := newKafkaEnv(t, nil)
	pool := k.mkPool(t, "defaults", func(d *streaming.CreateStreamPoolDetails) {
		d.KafkaSettings = &streaming.KafkaSettings{NumPartitions: new(3), LogRetentionHours: new(72)}
	})

	c, code := rawAuth(t, k, testTenancy+"/alice/"+*pool.Id, "tok")
	require.Equal(t, int16(0), code)

	req := kmsg.NewPtrCreateTopicsRequest()
	req.Version = 2
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic, topic.NumPartitions, topic.ReplicationFactor = "fromdefaults", -1, -1
	req.Topics = []kmsg.CreateTopicsRequestTopic{topic}

	got := rawCall(t, c, 1, req, kmsg.NewPtrCreateTopicsResponse())
	require.Equal(t, int16(0), got.Topics[0].ErrorCode)
	k.settle()

	list, err := k.admin.ListStreams(t.Context(), streamingListReq(*pool.Id))
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	st, err := k.admin.GetStream(t.Context(), streaming.GetStreamRequest{StreamId: list.Items[0].Id})
	require.NoError(t, err)
	assert.Equal(t, 3, *st.Partitions)
	assert.Equal(t, 72, *st.RetentionInHours)
}

func TestKafkaRequiresSASLBeforeData(t *testing.T) {
	t.Parallel()

	k := newKafkaEnv(t, nil)
	c := rawConn(t, k)

	req := kmsg.NewPtrMetadataRequest()
	req.Version = 1
	req.Topics = nil
	body := req.AppendTo(nil)
	hdr := binary.BigEndian.AppendUint16(nil, 3)
	hdr = binary.BigEndian.AppendUint16(hdr, 1)
	hdr = binary.BigEndian.AppendUint32(hdr, 7)
	hdr = binary.BigEndian.AppendUint16(hdr, 0xffff)
	frame := slices.Concat(hdr, body)
	out := binary.BigEndian.AppendUint32(nil, uint32(len(frame)))

	_, err := c.Write(append(out, frame...))
	require.NoError(t, err)
	require.NoError(t, c.SetReadDeadline(time.Now().Add(5*time.Second)))

	_, err = c.Read(make([]byte, 1))
	require.Error(t, err)
}

func TestKafkaCorruptBatchRejected(t *testing.T) {
	t.Parallel()

	k := newKafkaEnv(t, nil)
	s := k.mkStream(t, "t", 1)
	tests := []struct {
		mutate func([]byte) []byte
		name   string
	}{
		{name: "crc", mutate: func(b []byte) []byte { return withBytes(b, func(x []byte) { x[len(x)-1] ^= 0xff }) }},
		{name: "truncated", mutate: func(b []byte) []byte { return b[:len(b)-3] }},
		{name: "length", mutate: func(b []byte) []byte {
			return withBytes(b, func(x []byte) { x[8], x[9], x[10], x[11] = 0xff, 0xff, 0xff, 0xff })
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			batch := oci.EncodeTestBatch([]byte("v"))
			cl := k.client(t, *s.StreamPoolId, "tok")

			req := kmsg.NewPtrProduceRequest()
			req.Acks = 1
			req.TimeoutMillis = 1000
			req.Topics = []kmsg.ProduceRequestTopic{{Topic: "t", Partitions: []kmsg.ProduceRequestTopicPartition{
				{Partition: 0, Records: tt.mutate(bytes.Clone(batch))},
			}}}

			resp, err := req.RequestWith(t.Context(), cl)
			require.NoError(t, err)
			require.Len(t, resp.Topics, 1)
			assert.NotEqual(t, int16(0), resp.Topics[0].Partitions[0].ErrorCode)

			rng, rerr := k.backend.PartitionRanges(*s.Id)
			require.NoError(t, rerr)
			assert.Equal(t, int64(0), rng[0].End)
		})
	}
}

func TestKafkaConsumerGroupAssignsAndCommits(t *testing.T) {
	t.Parallel()

	k := newKafkaEnv(t, nil)
	s := k.mkStream(t, "g", 2)

	for p := range 2 {
		require.NoError(t, k.client(t, *s.StreamPoolId, "t").ProduceSync(t.Context(),
			&kgo.Record{Topic: "g", Partition: int32(p), Value: []byte("m")}).FirstErr())
	}

	var seen atomic.Int32

	cl := k.client(t, *s.StreamPoolId, "t", kgo.ConsumerGroup("cg"), kgo.ConsumeTopics("g"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())

	for seen.Load() < 2 {
		f := cl.PollFetches(t.Context())
		require.NoError(t, f.Err())
		f.EachRecord(func(*kgo.Record) { seen.Add(1) })
	}

	require.NoError(t, cl.CommitUncommittedOffsets(t.Context()))

	grp, err := k.admin.GetStreamPool(t.Context(), streamingGetPool(*s.StreamPoolId))
	require.NoError(t, err)
	_ = grp

	for p := range 2 {
		off, code := k.backend.KafkaCommittedForTest(*s.StreamPoolId, "g", "cg", p)
		assert.Equal(t, int16(0), code)
		assert.Equal(t, int64(1), off, "partition %d", p)
	}
}

func streamingGetPool(id string) streamingGetPoolReq { return streamingGetPoolReq{StreamPoolId: &id} }

type streamingGetPoolReq = streaming.GetStreamPoolRequest

func TestKafkaCreateAndDeleteTopics(t *testing.T) {
	t.Parallel()

	k := newKafkaEnv(t, nil)
	seed := k.mkStream(t, "seed", 1)
	cl := k.client(t, *seed.StreamPoolId, "tok")
	str := func(s string) *string { return &s }

	create := func(name string, parts int32, retention *string) int16 {
		req := kmsg.NewPtrCreateTopicsRequest()
		req.Version = 2
		topic := kmsg.NewCreateTopicsRequestTopic()
		topic.Topic, topic.NumPartitions, topic.ReplicationFactor = name, parts, 1

		if retention != nil {
			topic.Configs = []kmsg.CreateTopicsRequestTopicConfig{{Name: "retention.ms", Value: retention}}
		}

		req.Topics = []kmsg.CreateTopicsRequestTopic{topic}
		resp, err := req.RequestWith(t.Context(), cl)
		require.NoError(t, err)

		return resp.Topics[0].ErrorCode
	}

	assert.Equal(t, int16(0), create("made", 3, str("172800000")))
	assert.Equal(t, int16(36), create("made", 3, nil), "duplicate")
	assert.Equal(t, int16(37), create("big", 5000, nil), "partitions out of range")
	assert.Equal(t, int16(40), create("badcfg", 1, str("nope")), "retention.ms not a number")

	list, err := k.admin.ListStreams(t.Context(), streamingListReq(*seed.StreamPoolId))
	require.NoError(t, err)

	var made *streaming.StreamSummary

	for i := range list.Items {
		if *list.Items[i].Name == "made" {
			made = &list.Items[i]
		}
	}

	require.NotNil(t, made)
	assert.Equal(t, 3, *made.Partitions)

	got, err := k.admin.GetStream(t.Context(), streaming.GetStreamRequest{StreamId: made.Id})
	require.NoError(t, err)
	assert.Equal(t, 48, *got.RetentionInHours)

	del := func(name string) int16 {
		req := kmsg.NewPtrDeleteTopicsRequest()
		req.Version = 1
		req.TopicNames = []string{name}
		resp, derr := req.RequestWith(t.Context(), cl)
		require.NoError(t, derr)

		return resp.Topics[0].ErrorCode
	}

	k.settle()

	assert.Equal(t, int16(0), del("made"))
	assert.Equal(t, int16(3), del("made"), "already deleting")
	assert.Equal(t, int16(3), del("ghost"))
}

func streamingListReq(poolID string) streaming.ListStreamsRequest {
	return streaming.ListStreamsRequest{StreamPoolId: &poolID}
}

func withBytes(src []byte, fn func([]byte)) []byte {
	fn(src)

	return src
}
