package gcplro_test

import (
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
)

type tick struct{ t time.Time }

func (c *tick) now() time.Time { return c.t }

func putDone(t *testing.T, s *gcplro.Store, id string) {
	t.Helper()

	op, err := gcplro.Done(parent+"/operations/"+id, &emptypb.Empty{}, &emptypb.Empty{})
	require.NoError(t, err)
	s.Put(op)
}

func has(s *gcplro.Store, id string) bool {
	_, ok := s.Lookup(parent + "/operations/" + id)

	return ok
}

func TestRetention_CapDropsOldestFinished(t *testing.T) {
	t.Parallel()

	clock := &tick{t: time.Unix(1_000_000, 0)}
	s := gcplro.New("cap")
	s.SetClock(clock.now)
	s.SetLimits(20, 100*24*time.Hour)
	s.Put(&longrunningpb.Operation{Name: parent + "/operations/running"})

	for i := range 25 {
		clock.t = clock.t.Add(time.Second)
		putDone(t, s, fmt.Sprintf("op%02d", i))
	}

	assert.LessOrEqual(t, len(s.Under(parent+"/operations/")), 20)
	assert.True(t, has(s, "running"), "unfinished operations are never dropped")
	assert.True(t, has(s, "op24"), "the newest operation stays")
	assert.False(t, has(s, "op00"), "the oldest finished operation goes first")
}

func TestRetention_AgeSweepFollowsClock(t *testing.T) {
	t.Parallel()

	clock := &tick{t: time.Unix(1_000_000, 0)}
	s := gcplro.New("age")
	s.SetClock(clock.now)
	s.Put(&longrunningpb.Operation{Name: parent + "/operations/running"})
	putDone(t, s, "old")

	clock.t = clock.t.Add(gcplro.DefaultMaxAge - time.Hour)
	putDone(t, s, "mid")
	assert.True(t, has(s, "old"))

	clock.t = clock.t.Add(2 * time.Hour)
	putDone(t, s, "new")

	assert.False(t, has(s, "old"), "finished operations past the age limit are swept")
	assert.True(t, has(s, "mid"))
	assert.True(t, has(s, "running"))
}

func TestRetention_RestoreIsBounded(t *testing.T) {
	t.Parallel()

	src := gcplro.New("src")
	for i := range 30 {
		putDone(t, src, fmt.Sprintf("op%02d", i))
	}

	dst := gcplro.New("dst")
	dst.SetLimits(10, gcplro.DefaultMaxAge)
	require.NoError(t, dst.Restore(src.Snapshot()))

	assert.LessOrEqual(t, len(dst.Under(parent+"/operations/")), 10)
}
