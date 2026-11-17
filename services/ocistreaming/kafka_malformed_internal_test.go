package ocistreaming

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"net"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	offAttributes = 21
	offProducerID = 43
	offRecords    = batchHeaderSize
)

// resign rewrites the CRC so a deliberately altered batch is rejected for its content, not its checksum.
func resign(b []byte) []byte {
	binary.BigEndian.PutUint32(b[batchCRCOffset:batchCRCStart], crc32.Checksum(b[batchCRCStart:], castagnoli()))

	return b
}

func gzipBatch(t *testing.T, plain []byte) []byte {
	t.Helper()

	var zb bytes.Buffer

	zw := gzip.NewWriter(&zb)
	_, err := zw.Write(plain[offRecords:])
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	out := slices.Concat(plain[:offRecords], zb.Bytes())
	binary.BigEndian.PutUint16(out[offAttributes:], compressionGzip)
	binary.BigEndian.PutUint32(out[8:12], uint32(len(out)-batchPrefixSize))

	return resign(out)
}

func TestSplitBatchesRejectsByKind(t *testing.T) {
	t.Parallel()

	good := encodeBatch([]Message{{Value: []byte("v")}}, 0)
	bomb := encodeBatch([]Message{{Value: make([]byte, 3<<20)}}, 0)

	tests := []struct {
		want   error
		name   string
		data   []byte
		budget int64
	}{
		{name: "idempotent producer", want: errUnsupported, budget: maxBatchInflated,
			data: resign(mutated(good, func(b []byte) { b[offProducerID] = 0 }))},
		{name: "transactional", want: errUnsupported, budget: maxBatchInflated,
			data: resign(mutated(good, func(b []byte) { b[offAttributes+1] |= attrTransactional }))},
		{name: "control batch", want: errUnsupported, budget: maxBatchInflated,
			data: resign(mutated(good, func(b []byte) { b[offAttributes+1] |= attrControl }))},
		{name: "snappy", want: errCompression, budget: maxBatchInflated,
			data: resign(mutated(good, func(b []byte) { b[offAttributes+1] = 2 }))},
		{name: "zstd", want: errCompression, budget: maxBatchInflated,
			data: resign(mutated(good, func(b []byte) { b[offAttributes+1] = 4 }))},
		{name: "gzip bomb", want: errBatchTooLarge, budget: 1 << 20, data: gzipBatch(t, bomb)},
		{name: "gzip over remaining budget", want: errBatchTooLarge, budget: 5, data: gzipBatch(t, good)},
		{name: "gzip garbage", want: errBadBatch, budget: maxBatchInflated,
			data: resign(mutated(good, func(b []byte) { b[offAttributes+1] = compressionGzip }))},
		{name: "record count over cap", want: errBadBatch, budget: maxBatchInflated,
			data: resign(mutated(good, func(b []byte) { binary.BigEndian.PutUint32(b[57:61], maxRecordsPerBatch+1) }))},
		{name: "negative record count", want: errBadBatch, budget: maxBatchInflated,
			data: resign(mutated(good, func(b []byte) { binary.BigEndian.PutUint32(b[57:61], 0xffffffff) }))},
		{name: "record count over records", want: errBadBatch, budget: maxBatchInflated,
			data: resign(mutated(good, func(b []byte) { binary.BigEndian.PutUint32(b[57:61], 5) }))},
		{name: "length below header", want: errBadBatch, budget: maxBatchInflated,
			data: mutated(good, func(b []byte) { binary.BigEndian.PutUint32(b[8:12], 10) })},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			budget := tt.budget
			_, err := splitBatches(tt.data, &budget)
			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestSplitBatchesGzipRoundTrip(t *testing.T) {
	t.Parallel()

	plain := encodeBatch([]Message{{Key: []byte("k"), Value: []byte("v")}}, 0)
	budget := int64(maxBatchInflated)

	got, err := splitBatches(gzipBatch(t, plain), &budget)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []byte("v"), got[0].Value)
}

func TestSplitBatchesDoesNotRetainFrame(t *testing.T) {
	t.Parallel()

	frame := encodeBatch([]Message{{Key: []byte("key"), Value: []byte("value")}}, 0)
	budget := int64(maxBatchInflated)

	got, err := splitBatches(frame, &budget)
	require.NoError(t, err)

	clear(frame)
	assert.Equal(t, []byte("key"), got[0].Key, "stored messages must not alias the request frame")
	assert.Equal(t, []byte("value"), got[0].Value)
}

func TestParseHeaderMalformed(t *testing.T) {
	t.Parallel()

	hdr := func(idLen int16, id string) []byte {
		b := binary.BigEndian.AppendUint16(nil, 3)
		b = binary.BigEndian.AppendUint16(b, 1)
		b = binary.BigEndian.AppendUint32(b, 9)
		b = binary.BigEndian.AppendUint16(b, uint16(idLen))

		return append(b, id...)
	}

	tests := []struct {
		name  string
		frame []byte
		ok    bool
	}{
		{name: "empty"},
		{name: "short", frame: make([]byte, 9)},
		{name: "null client id", frame: hdr(-1, ""), ok: true},
		{name: "client id", frame: hdr(3, "abc"), ok: true},
		{name: "client id past frame", frame: hdr(40, "abc")},
		{name: "int16 max client id", frame: hdr(32767, "abc")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := parseHeader(tt.frame)
			assert.Equal(t, tt.ok, err == nil)
		})
	}
}

func TestReadKafkaFrame(t *testing.T) {
	t.Parallel()

	size := func(n uint32, body int) io.Reader {
		return bytes.NewReader(append(binary.BigEndian.AppendUint32(nil, n), make([]byte, body)...))
	}

	tests := []struct {
		r     io.Reader
		name  string
		limit uint32
		ok    bool
	}{
		{name: "at limit", r: size(10, 10), limit: 10, ok: true},
		{name: "over limit", r: size(11, 11), limit: 10},
		{name: "zero", r: size(0, 0), limit: 10},
		{name: "short size", r: bytes.NewReader([]byte{0, 0}), limit: 10},
		{name: "short body", r: size(8, 3), limit: 10},
		{name: "max uint32", r: size(1<<32-1, 0), limit: kafkaMaxFrame},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := readKafkaFrame(tt.r, tt.limit)
			assert.Equal(t, tt.ok, err == nil)
		})
	}
}

func newStepConn(t *testing.T) *kconn {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	srv, cli := net.Pipe()

	go func() { _, _ = io.Copy(io.Discard, cli) }()

	t.Cleanup(func() { _ = srv.Close(); _ = cli.Close() })

	h := NewHandler(NewInMemoryBackend())
	k := &kafkaServer{h: h, groups: newCoordinator(h.Backend.nowUTC), ctx: ctx}

	return &kconn{k: k, conn: srv, authed: true, poolID: "pool"}
}

func frameFor(key, version int16, body []byte) []byte {
	b := binary.BigEndian.AppendUint16(nil, uint16(key))
	b = binary.BigEndian.AppendUint16(b, uint16(version))
	b = binary.BigEndian.AppendUint32(b, 1)
	b = binary.BigEndian.AppendUint16(b, 0xffff)

	return append(b, body...)
}

func TestStepSurvivesMalformedBodies(t *testing.T) {
	t.Parallel()

	for key, rng := range kafkaAPIs() {
		for v := rng.min; v <= rng.max; v++ {
			valid := kmsg.RequestForKey(key)
			valid.SetVersion(v)

			full := valid.AppendTo(nil)
			bodies := map[string][]byte{
				"empty": nil, "ones": bytes.Repeat([]byte{0xff}, 64), "max ints": bytes.Repeat([]byte{0x7f}, 200),
				"zeros": make([]byte, 64), "huge count": slices.Concat(make([]byte, 6), []byte{0x7f, 0xff, 0xff, 0xff}),
			}

			for cut := range full {
				bodies["cut"+itoa(cut)] = full[:cut]
			}

			for name, body := range bodies {
				t.Run(itoa(int(key))+"/"+itoa(int(v))+"/"+name, func(t *testing.T) {
					t.Parallel()

					c := newStepConn(t)
					assert.NotPanics(t, func() { c.step(frameFor(key, v, body)) })
				})
			}
		}
	}
}

func TestStepRejectsDataBeforeAuth(t *testing.T) {
	t.Parallel()

	for key := range kafkaAPIs() {
		if key == apiVersions || key == apiSaslHandshake || key == apiSaslAuthenticate {
			continue
		}

		t.Run(itoa(int(key)), func(t *testing.T) {
			t.Parallel()

			c := newStepConn(t)
			c.authed = false

			req := kmsg.RequestForKey(key)
			req.SetVersion(kafkaAPIs()[key].min)

			assert.False(t, c.step(frameFor(key, req.GetVersion(), req.AppendTo(nil))))
		})
	}
}

func joinAsync(
	ctx context.Context, c *coordinator, key string, r *kmsg.JoinGroupRequest,
) <-chan *kmsg.JoinGroupResponse {
	done := make(chan *kmsg.JoinGroupResponse, 1)

	go func() { done <- c.join(ctx, key, r) }()

	return done
}

func TestCoordinatorSecondMemberRebalances(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		c := newCoordinator(time.Now)
		key := groupMapKey("pool", "g")
		m1 := c.join(t.Context(), key, joinReq("g", ""))

		done := joinAsync(t.Context(), c, key, joinReq("g", ""))
		synctest.Wait()

		assert.Equal(t, kerrRebalanceInProgress, c.heartbeat(key, m1.MemberID, m1.Generation))

		r1 := c.join(t.Context(), key, joinReq("g", m1.MemberID))
		r2 := <-done

		assert.Equal(t, int32(2), r1.Generation)
		assert.Equal(t, r1.Generation, r2.Generation)
		assert.Equal(t, r1.LeaderID, r2.LeaderID)
		assert.NotEqual(t, r1.MemberID, r2.MemberID)
	})
}

func TestCoordinatorRebalanceTimeoutDropsSilentMember(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		for range 24 {
			dropSilentMember(t)
		}
	})
}

// dropSilentMember runs one round; member ids are random, so the loop above covers both id orderings.
func dropSilentMember(t *testing.T) {
	t.Helper()

	{
		c := newCoordinator(time.Now)
		key := groupMapKey("pool", "g")
		m1 := c.join(t.Context(), key, joinReq("g", ""))

		done := joinAsync(t.Context(), c, key, joinReq("g", ""))
		synctest.Wait()

		select {
		case <-done:
			t.Fatal("the join answered before the rebalance timeout")
		default:
		}

		time.Sleep(11 * time.Second)
		synctest.Wait()

		r2 := <-done
		assert.Equal(t, int16(0), r2.ErrorCode)
		assert.Equal(t, int32(2), r2.Generation)
		assert.Equal(t, r2.MemberID, r2.LeaderID, "the member that never rejoined is dropped")
		assert.Equal(t, kerrUnknownMember, c.heartbeat(key, m1.MemberID, m1.Generation))
	}
}

func TestCoordinatorRebalanceTimeoutIsBounded(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		c := newCoordinator(time.Now)
		key := groupMapKey("pool", "g")
		c.join(t.Context(), key, joinReq("g", ""))

		r := joinReq("g", "")
		r.RebalanceTimeoutMillis = 1 << 30
		done := joinAsync(t.Context(), c, key, r)
		synctest.Wait()

		time.Sleep(maxRebalanceTimeout + time.Second)
		synctest.Wait()

		select {
		case got := <-done:
			assert.Equal(t, int16(0), got.ErrorCode)
		default:
			t.Fatal("a huge rebalance timeout must be clamped")
		}
	})
}

func TestCoordinatorDropsGroupWhenEveryoneLeaves(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		c := newCoordinator(time.Now)
		key := groupMapKey("pool", "g")
		m1 := c.join(t.Context(), key, joinReq("g", ""))

		done := joinAsync(t.Context(), c, key, joinReq("g", ""))
		synctest.Wait()

		var second string

		c.mu.RLock("test")
		for id := range c.groups[key].members {
			if id != m1.MemberID {
				second = id
			}
		}
		c.mu.RUnlock()

		assert.Equal(t, []int16{0}, c.leave(key, []string{second}))
		assert.Equal(t, []int16{0}, c.leave(key, []string{m1.MemberID}))

		got := <-done
		assert.Equal(t, kerrUnknownMember, got.ErrorCode)

		c.mu.RLock("test")
		defer c.mu.RUnlock()

		assert.Empty(t, c.groups)
	})
}

func TestCoordinatorSyncWaitsForLeader(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		c := newCoordinator(time.Now)
		key := groupMapKey("pool", "g")
		m1 := c.join(t.Context(), key, joinReq("g", ""))

		other := joinAsync(t.Context(), c, key, joinReq("g", ""))
		synctest.Wait()

		r1 := c.join(t.Context(), key, joinReq("g", m1.MemberID))
		r2 := <-other

		follower := r2
		leader := r1

		if r1.LeaderID != r1.MemberID {
			follower, leader = r1, r2
		}

		syncReq := func(r *kmsg.JoinGroupResponse, assign []kmsg.SyncGroupRequestGroupAssignment) *kmsg.SyncGroupRequest {
			s := kmsg.NewPtrSyncGroupRequest()
			s.Group, s.MemberID, s.Generation, s.GroupAssignment = "g", r.MemberID, r.Generation, assign

			return s
		}

		got := make(chan *kmsg.SyncGroupResponse, 1)

		go func() { got <- c.sync(t.Context(), key, syncReq(follower, nil)) }()

		synctest.Wait()

		select {
		case <-got:
			t.Fatal("a follower's sync must wait for the leader's assignment")
		default:
		}

		lead := c.sync(t.Context(), key, syncReq(leader, []kmsg.SyncGroupRequestGroupAssignment{
			{MemberID: follower.MemberID, MemberAssignment: []byte("f")},
			{MemberID: leader.MemberID, MemberAssignment: []byte("l")},
		}))

		assert.Equal(t, []byte("l"), lead.MemberAssignment)
		assert.Equal(t, []byte("f"), (<-got).MemberAssignment)
	})
}

func TestFetchLongPollStopsOnShutdown(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := NewInMemoryBackend()
		b.SetTransition(0)

		w, _, _, err := b.CreateStream(CreateStreamIn{Name: "t", Compartment: "c", Partitions: new(1)}, "")
		require.NoError(t, err)
		b.Sweep()

		h := NewHandler(b)
		ctx, cancel := context.WithCancel(t.Context())
		c := &kconn{k: &kafkaServer{h: h, ctx: ctx}, poolID: b.streams[w.ID].PoolID}

		req := kmsg.NewPtrFetchRequest()
		req.Version, req.MaxWaitMillis = 4, 20000
		req.Topics = []kmsg.FetchRequestTopic{{Topic: "t", Partitions: []kmsg.FetchRequestTopicPartition{{}}}}

		done := make(chan struct{})

		go func() {
			defer close(done)

			c.fetch(ctx, req)
		}()

		synctest.Wait()

		select {
		case <-done:
			t.Fatal("the long poll returned before its wait or a shutdown")
		default:
		}

		cancel()
		synctest.Wait()

		select {
		case <-done:
		default:
			t.Fatal("shutdown must wake the long poll without waiting for its timer")
		}
	})
}

func TestCoordinatorTimeoutWithEmptyGroupDropsIt(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		c := newCoordinator(time.Now)
		key := groupMapKey("pool", "g")
		m1 := c.join(t.Context(), key, joinReq("g", ""))

		done := joinAsync(t.Context(), c, key, joinReq("g", ""))
		synctest.Wait()

		var second string

		c.mu.RLock("test")
		for id := range c.groups[key].members {
			if id != m1.MemberID {
				second = id
			}
		}
		c.mu.RUnlock()

		assert.Equal(t, []int16{0}, c.leave(key, []string{second}))

		time.Sleep(11 * time.Second)
		synctest.Wait()

		assert.Equal(t, kerrUnknownMember, (<-done).ErrorCode)

		c.mu.RLock("test")
		defer c.mu.RUnlock()

		assert.Empty(t, c.groups, "nobody rejoined, so the round ends with no members and no group")
	})
}
