package ocistreaming

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type testClock struct {
	t  time.Time
	mu sync.Mutex
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func joinReq(group, member string) *kmsg.JoinGroupRequest {
	r := kmsg.NewPtrJoinGroupRequest()
	r.Version, r.Group, r.MemberID, r.ProtocolType = 3, group, member, "consumer"
	r.SessionTimeoutMillis, r.RebalanceTimeoutMillis = 10000, 10000
	r.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: []byte{1}}}

	return r
}

func TestCoordinatorLifecycle(t *testing.T) {
	t.Parallel()

	clk := &testClock{t: time.Unix(1000, 0)}
	c := newCoordinator(clk.now)
	key := groupMapKey("pool", "g")

	first := c.join(t.Context(), key, joinReq("g", ""))
	require.Equal(t, int16(0), first.ErrorCode)
	assert.Equal(t, int32(1), first.Generation)
	assert.Equal(t, first.MemberID, first.LeaderID)
	require.Len(t, first.Members, 1)

	sy := kmsg.NewPtrSyncGroupRequest()
	sy.Group, sy.MemberID, sy.Generation = "g", first.MemberID, first.Generation
	sy.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
		{MemberID: first.MemberID, MemberAssignment: []byte("a1")},
	}
	sres := c.sync(t.Context(), key, sy)
	require.Equal(t, int16(0), sres.ErrorCode)
	assert.Equal(t, []byte("a1"), sres.MemberAssignment)

	assert.Equal(t, int16(0), c.heartbeat(key, first.MemberID, first.Generation))
	assert.Equal(t, kerrIllegalGeneration, c.heartbeat(key, first.MemberID, 99))
	assert.Equal(t, kerrUnknownMember, c.heartbeat(key, "nobody", 1))

	clk.advance(time.Minute)
	c.sweep()
	assert.Equal(
		t,
		kerrUnknownMember,
		c.heartbeat(key, first.MemberID, first.Generation),
		"session expiry on the backend clock",
	)
	assert.Empty(t, c.groups)
}

func TestCoordinatorSecondMemberRebalances(t *testing.T) {
	t.Parallel()

	clk := &testClock{t: time.Unix(1000, 0)}
	c := newCoordinator(clk.now)
	key := groupMapKey("pool", "g")
	m1 := c.join(t.Context(), key, joinReq("g", ""))

	done := make(chan *kmsg.JoinGroupResponse, 1)

	go func() { done <- c.join(t.Context(), key, joinReq("g", "")) }()

	for {
		if code := c.heartbeat(key, m1.MemberID, m1.Generation); code == kerrRebalanceInProgress {
			break
		}
	}

	r1 := c.join(t.Context(), key, joinReq("g", m1.MemberID))
	r2 := <-done

	assert.Equal(t, int32(2), r1.Generation)
	assert.Equal(t, r1.Generation, r2.Generation)
	assert.Equal(t, r1.LeaderID, r2.LeaderID)
	assert.NotEqual(t, r1.MemberID, r2.MemberID)
}

func TestCoordinatorBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*kmsg.JoinGroupRequest)
		name   string
		want   int16
	}{
		{name: "empty group", mutate: func(r *kmsg.JoinGroupRequest) { r.Group = "" }, want: kerrInvalidGroupID},
		{
			name:   "no protocols",
			mutate: func(r *kmsg.JoinGroupRequest) { r.Protocols = nil },
			want:   kerrInconsistentProto,
		},
		{name: "short session", mutate: func(r *kmsg.JoinGroupRequest) { r.SessionTimeoutMillis = 10 },
			want: kerrInvalidSession},
		{name: "long session", mutate: func(r *kmsg.JoinGroupRequest) { r.SessionTimeoutMillis = 1 << 30 },
			want: kerrInvalidSession},
		{
			name:   "unknown member",
			mutate: func(r *kmsg.JoinGroupRequest) { r.MemberID = "ghost" },
			want:   kerrUnknownMember,
		},
		{name: "huge metadata", want: kerrInconsistentProto,
			mutate: func(r *kmsg.JoinGroupRequest) { r.Protocols[0].Metadata = make([]byte, maxMemberMetadataSize+1) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newCoordinator((&testClock{t: time.Unix(1, 0)}).now)
			r := joinReq("g", "")
			tt.mutate(r)
			assert.Equal(t, tt.want, c.join(t.Context(), groupMapKey("p", r.Group), r).ErrorCode)
		})
	}
}

func TestCoordinatorGroupCap(t *testing.T) {
	t.Parallel()

	c := newCoordinator((&testClock{t: time.Unix(1, 0)}).now)

	for i := range maxKafkaGroups {
		g := "g" + itoa(i)
		require.Equal(t, int16(0), c.join(t.Context(), groupMapKey("p", g), joinReq(g, "")).ErrorCode)
	}

	assert.Equal(t, kerrCoordinatorNA, c.join(t.Context(), groupMapKey("p", "extra"), joinReq("extra", "")).ErrorCode)
}

func TestCoordinatorConcurrentJoinLeave(t *testing.T) {
	t.Parallel()

	c := newCoordinator((&testClock{t: time.Unix(1, 0)}).now)
	key := groupMapKey("p", "g")
	ctx, cancel := context.WithCancel(t.Context())

	var wg sync.WaitGroup

	for range 8 {
		wg.Go(func() {
			r := c.join(ctx, key, joinReq("g", ""))
			c.heartbeat(key, r.MemberID, r.Generation)
			c.leave(key, []string{r.MemberID})
		})
	}

	wg.Wait()
	cancel()
	c.sweep()
	assert.Empty(t, c.groups)
}

func TestSplitBatchesMalformed(t *testing.T) {
	t.Parallel()

	good := encodeBatch([]Message{{Value: []byte("v")}}, 0)
	flip := func(i int) []byte {
		b := bytes.Clone(good)
		b[i] ^= 0xff

		return b
	}

	tests := []struct {
		name string
		data []byte
		ok   bool
	}{
		{name: "good", data: good, ok: true},
		{name: "empty", data: nil, ok: true},
		{name: "short prefix", data: good[:5]},
		{name: "truncated", data: good[:len(good)-1]},
		{name: "magic", data: flip(16)},
		{name: "crc field", data: flip(18)},
		{name: "payload", data: flip(len(good) - 2)},
		{name: "attributes compression", data: mutated(good, func(b []byte) { b[22] = 3 })},
		{name: "length overflow", data: mutated(good, func(b []byte) { b[8], b[9] = 0x7f, 0xff })},
		{name: "zeros", data: make([]byte, 80)},
		{name: "trailing junk", data: append(bytes.Clone(good), 1, 2, 3)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			budget := int64(maxBatchInflated)
			_, err := splitBatches(tt.data, &budget)

			if tt.ok {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
		})
	}
}

func TestSplitBatchesBudgetShared(t *testing.T) {
	t.Parallel()

	b := encodeBatch([]Message{{Value: make([]byte, 100)}}, 0)
	budget := int64(150)
	_, err := splitBatches(append(bytes.Clone(b), b...), &budget)

	require.ErrorIs(t, err, errBatchTooLarge)
}

func TestKafkaCommitSurvivesSnapshot(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	b.SetTransition(0)

	w, _, _, err := b.CreateStream(CreateStreamIn{Name: "s", Compartment: "c", Partitions: new(2)}, "")
	require.NoError(t, err)
	b.Sweep()

	s := b.streams[w.ID]
	_, code := b.kafkaAppend(s.PoolID, "s", 1, []putEntryIn{{Value: []byte("a")}, {Value: []byte("b")}})
	require.Equal(t, int16(0), code)
	require.Equal(t, int16(0), b.kafkaCommit(s.PoolID, "s", "g", 1, 2))
	assert.Equal(t, kerrInvalidCommitOffset, b.kafkaCommit(s.PoolID, "s", "g", 1, 3))

	snap := b.Snapshot(t.Context())
	b2 := NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	off, code := b2.kafkaCommitted(s.PoolID, "s", "g", 1)
	assert.Equal(t, int16(0), code)
	assert.Equal(t, int64(2), off)
}

func TestKafkaAppendLimitsConcurrent(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	b.SetTransition(0)

	w, _, _, err := b.CreateStream(CreateStreamIn{Name: "s", Compartment: "c", Partitions: new(1)}, "")
	require.NoError(t, err)
	b.Sweep()

	pool := b.streams[w.ID].PoolID
	chunk := []putEntryIn{{Value: make([]byte, 200<<10)}}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		accepted int
	)

	for range 20 {
		wg.Go(func() {
			if _, code := b.kafkaAppend(pool, "s", 0, chunk); code == kerrNone {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		})
	}

	wg.Wait()
	assert.LessOrEqual(t, accepted*200<<10, maxPartitionBytes, "1 MiB/s partition ingest limit holds under concurrency")
	assert.Positive(t, accepted)
}

func TestKafkaFetchWakesOnAppend(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	sig := b.appendSignal()

	select {
	case <-sig:
		t.Fatal("signalled before any append")
	default:
	}

	b.SetTransition(0)

	w, _, _, err := b.CreateStream(CreateStreamIn{Name: "s", Compartment: "c", Partitions: new(1)}, "")
	require.NoError(t, err)
	b.Sweep()

	sig = b.appendSignal()
	_, code := b.kafkaAppend(b.streams[w.ID].PoolID, "s", 0, []putEntryIn{{Value: []byte("a")}})
	require.Equal(t, int16(0), code)

	select {
	case <-sig:
	default:
		t.Fatal("append did not wake the long poll")
	}
}

func TestPartitionAllowance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		parts     []int
		wantCodes []string
		limit     int
	}{
		{name: "within", limit: 4, parts: []int{2, 2}, wantCodes: []string{"", ""}},
		{name: "exceeds", limit: 4, parts: []int{2, 2, 1}, wantCodes: []string{"", "", "LimitExceeded"}},
		{name: "single over", limit: 4, parts: []int{5}, wantCodes: []string{"LimitExceeded"}},
		{name: "disabled", limit: 0, parts: []int{300, 300}, wantCodes: []string{"", ""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend()
			b.SetPartitionLimit(tt.limit)

			for i, p := range tt.parts {
				_, _, _, err := b.CreateStream(
					CreateStreamIn{Name: "s" + itoa(i), Compartment: "c", Partitions: new(p)},
					"",
				)
				if tt.wantCodes[i] == "" {
					require.NoError(t, err)

					continue
				}

				var ae *APIError
				require.ErrorAs(t, err, &ae)
				assert.Equal(t, tt.wantCodes[i], ae.Code)
				assert.Equal(t, 400, ae.Status)
			}
		})
	}
}

func TestPartitionAllowanceReleasedOnDelete(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	b.SetPartitionLimit(2)
	b.SetTransition(0)

	w, _, _, err := b.CreateStream(CreateStreamIn{Name: "a", Compartment: "c", Partitions: new(2)}, "")
	require.NoError(t, err)
	b.Sweep()

	_, _, _, err = b.CreateStream(CreateStreamIn{Name: "b", Compartment: "c", Partitions: new(1)}, "")
	require.Error(t, err)

	_, err = b.DeleteStream(w.ID, "")
	require.NoError(t, err)
	b.Sweep()

	_, _, _, err = b.CreateStream(CreateStreamIn{Name: "b", Compartment: "c", Partitions: new(2)}, "")
	require.NoError(t, err)
}

func TestPartitionAllowanceConcurrent(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	b.SetPartitionLimit(10)

	var (
		wg sync.WaitGroup
		ok int64
		mu sync.Mutex
	)

	for i := range 40 {
		wg.Go(func() {
			if _, _, _, err := b.CreateStream(
				CreateStreamIn{Name: "s" + itoa(i), Compartment: "c", Partitions: new(1)},
				"",
			); err == nil {
				mu.Lock()
				ok++
				mu.Unlock()
			}
		})
	}

	wg.Wait()
	assert.Equal(t, int64(10), ok)
}

func mutated(src []byte, fn func([]byte)) []byte {
	b := bytes.Clone(src)
	fn(b)

	return b
}
