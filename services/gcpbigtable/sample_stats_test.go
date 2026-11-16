package gcpbigtable_test

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpbigtable"
)

func recvSamples(t *testing.T, e *env) []*btpb.SampleRowKeysResponse {
	t.Helper()

	stream, err := btpb.NewBigtableClient(e.conn).
		SampleRowKeys(t.Context(), &btpb.SampleRowKeysRequest{TableName: tablePath})
	require.NoError(t, err)

	var got []*btpb.SampleRowKeysResponse

	for {
		r, rerr := stream.Recv()
		if errors.Is(rerr, io.EOF) {
			return got
		}

		require.NoError(t, rerr)

		got = append(got, r)
	}
}

func TestSampleRowKeysBySize(t *testing.T) {
	t.Parallel()

	const (
		stride  = 1 << 20
		rowSize = 100 * 1024
		rows    = 40
	)

	e := newEnv(t)
	tbl := e.seeded(t, "t", "cf")
	val := strings.Repeat("x", rowSize)

	for i := range rows {
		put(t, tbl, seedCell{fmt.Sprintf("r%03d", i), "cf", "q", val, 1000})
	}

	got := recvSamples(t, e)
	require.Greater(t, len(got), 3, "a table of several strides has size-based samples")
	assert.Empty(t, got[len(got)-1].GetRowKey())
	assert.GreaterOrEqual(t, got[len(got)-1].GetOffsetBytes(), int64(rows*rowSize))

	prev := got[0]
	assert.GreaterOrEqual(t, prev.GetOffsetBytes(), int64(stride))

	for _, s := range got[1 : len(got)-1] {
		assert.Greater(t, string(s.GetRowKey()), string(prev.GetRowKey()))
		assert.GreaterOrEqual(t, s.GetOffsetBytes()-prev.GetOffsetBytes(), int64(stride))

		prev = s
	}
}

func TestSampleRowKeysSmallTable(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tbl := e.seeded(t, "t", "cf")
	put(t, tbl, seedCell{"a", "cf", "q", "v", 1000})

	got := recvSamples(t, e)
	require.Len(t, got, 1, "a table under one stride has only the end marker")
	assert.Empty(t, got[0].GetRowKey())
}

type stepClock struct{ now time.Time }

func (c *stepClock) tick() time.Time {
	c.now = c.now.Add(7 * time.Millisecond)

	return c.now
}

func TestReadRowsFrontendLatency(t *testing.T) {
	t.Parallel()

	clk := &stepClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b := gcpbigtable.NewInMemoryBackend()
	b.SetClock(clk.tick)

	e := newEnvWithBackend(t, b)
	tbl := e.seeded(t, "t", "cf")
	put(t, tbl, seedCell{"a", "cf", "q", "v", 1000})

	stream, err := btpb.NewBigtableClient(e.conn).ReadRows(t.Context(), &btpb.ReadRowsRequest{
		TableName: tablePath, RequestStatsView: btpb.ReadRowsRequest_REQUEST_STATS_FULL,
	})
	require.NoError(t, err)

	var latency time.Duration

	found := false

	for {
		r, rerr := stream.Recv()
		if errors.Is(rerr, io.EOF) {
			break
		}

		require.NoError(t, rerr)

		if st := r.GetRequestStats().GetFullReadStatsView(); st != nil {
			found = true
			latency = st.GetRequestLatencyStats().GetFrontendServerLatency().AsDuration()
		}
	}

	require.True(t, found)
	assert.Positive(t, latency, "the latency is measured, not a constant zero")
	assert.Zero(t, latency%(7*time.Millisecond), "it is a whole number of clock steps")
}
