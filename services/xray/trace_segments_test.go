package xray_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

func TestInMemoryBackend_PutTraceSegments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		segments        []string
		wantUnprocessed int
	}{
		{
			name:            "valid segment stored",
			segments:        []string{`{"trace_id":"1-abc123","id":"seg1","name":"test"}`},
			wantUnprocessed: 0,
		},
		{
			name:            "invalid JSON becomes unprocessed",
			segments:        []string{"not-json"},
			wantUnprocessed: 1,
		},
		{
			name:            "missing trace_id becomes unprocessed",
			segments:        []string{`{"id":"seg1","name":"test"}`},
			wantUnprocessed: 1,
		},
		{
			name:            "empty segments",
			segments:        []string{},
			wantUnprocessed: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			unprocessed := b.PutTraceSegments(tt.segments)
			assert.Len(t, unprocessed, tt.wantUnprocessed)
		})
	}
}

func TestInMemoryBackend_PutTraceSegmentsCap(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	const traceID = "1-58406520-a006649127e371903a2de979"
	segTemplate := `{"trace_id":"` + traceID + `","id":"%d"}`

	total := xray.SegmentCompactionHighWater + 50
	segs := make([]string, 0, total)

	for i := range total {
		segs = append(segs, fmt.Sprintf(segTemplate, i))
	}

	_ = b.PutTraceSegments(segs)

	got := b.GetTrace(traceID)
	require.NotNil(t, got, "trace must exist")

	// After amortized compaction the slice length is bounded by 2x the cap;
	// the cap itself is the floor maintained between compactions.
	assert.LessOrEqual(t, len(got.Segments), xray.SegmentCompactionHighWater,
		"segments must never exceed compaction high-water mark")
	assert.GreaterOrEqual(t, len(got.Segments), xray.MaxSegmentsPerTrace,
		"segments must retain at least the cap")
}
