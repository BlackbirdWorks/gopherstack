package xray

import (
	"encoding/json"
	"math"
	"time"

	"github.com/google/uuid"
)

// PutTraceSegments stores raw segment JSON strings, parses them into typed Segment structs,
// and returns the list of unprocessed segment IDs (empty slice means all segments were accepted).
func (b *InMemoryBackend) PutTraceSegments(segments []string) []string {
	b.mu.Lock("PutTraceSegments")
	defer b.mu.Unlock()

	unprocessed := make([]string, 0, len(segments))
	newlyParsed := make([]*Segment, 0, len(segments))

	for _, seg := range segments {
		var hdr segmentHeader
		if err := json.Unmarshal([]byte(seg), &hdr); err != nil || hdr.TraceID == "" {
			unprocessed = append(unprocessed, uuid.NewString())

			continue
		}

		t, ok := b.traces.Get(hdr.TraceID)
		if !ok {
			t = &Trace{
				TraceID:   hdr.TraceID,
				StartTime: time.Now(),
				Segments:  []string{},
			}
			b.traces.Put(t)
		}

		// Cap per-trace segment count.
		if len(t.Segments) >= segmentCompactionHighWater {
			trimmed := make([]string, maxSegmentsPerTrace, segmentCompactionHighWater)
			copy(trimmed, t.Segments[len(t.Segments)-maxSegmentsPerTrace:])
			t.Segments = trimmed
		}

		t.Segments = append(t.Segments, seg)

		// Parse into typed Segment struct and index it.
		var parsed Segment
		if err := json.Unmarshal([]byte(seg), &parsed); err == nil {
			parsed.Document = seg

			b.parsedSegments.Put(&parsed)
			newlyParsed = append(newlyParsed, &parsed)

			// Update trace StartTime from the earliest segment start_time.
			if parsed.StartTime > 0 {
				segStart := time.Unix(
					int64(parsed.StartTime),
					int64((parsed.StartTime-math.Floor(parsed.StartTime))*nanosPerSecond),
				)
				if segStart.Before(t.StartTime) {
					t.StartTime = segStart
				}
			}
		}
	}

	b.detectInsights(newlyParsed)

	return unprocessed
}

const (
	// maxSegmentsPerTrace caps the number of raw segment payloads stored for a
	// single trace so one runaway producer cannot consume unbounded memory
	// before the janitor's TTL sweep removes the trace.
	maxSegmentsPerTrace = 5000
)

const (
	// segmentCompactionHighWater is the slice length that triggers
	// compaction. Compacting only when the slice has grown to twice the cap
	// keeps the per-call cost amortized O(1).
	segmentCompactionHighWater = maxSegmentsPerTrace + maxSegmentsPerTrace
)

const (
	// nanosPerSecond is the number of nanoseconds in a second.
	nanosPerSecond = 1e9
)
