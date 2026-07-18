package xray_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

// TestInsightDetection_FaultRateTriggers verifies that ingesting enough fault
// segments for a service creates an ACTIVE insight automatically.
func TestInsightDetection_FaultRateTriggers(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")

	const traceID = "1-fault-trace-0000000000000001"
	const svcName = "fault-service"

	// Send 20 segments: 10 fault, 10 ok — 50% fault rate, above 5% threshold.
	segs := make([]string, 0, 20)
	for range 10 {
		segs = append(segs, buildFaultSegmentJSON(t, traceID, svcName, true))
	}
	for range 10 {
		segs = append(segs, buildFaultSegmentJSON(t, traceID, svcName, false))
	}

	unprocessed := b.PutTraceSegments(segs)
	assert.Empty(t, unprocessed, "all segments should be processed")

	// An insight should have been created.
	assert.Positive(t, b.InsightCount(), "expected at least one insight to be auto-detected")

	summaries, err := b.GetInsightSummaries([]string{"ACTIVE"})
	require.NoError(t, err)
	assert.NotEmpty(t, summaries, "expected active insight summaries")
	assert.Equal(t, "ACTIVE", summaries[0].State)
}

// TestInsightDetection_NoInsightBelowThreshold verifies that no insight is
// created when the fault rate is below the threshold.
func TestInsightDetection_NoInsightBelowThreshold(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")

	const traceID = "1-ok-trace-00000000000000001"
	const svcName = "healthy-service"

	// 20 ok segments — 0% fault rate.
	segs := make([]string, 0, 20)
	for range 20 {
		segs = append(segs, buildFaultSegmentJSON(t, traceID, svcName, false))
	}

	unprocessed := b.PutTraceSegments(segs)
	assert.Empty(t, unprocessed)

	// No insights should be created.
	assert.Equal(t, 0, b.InsightCount(), "expected no insights for healthy service")
}

// TestAddInsightEventInternal verifies the seed helper works.
func TestAddInsightEventInternal(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddInsightInternal(xray.Insight{InsightID: "ins-1", State: "ACTIVE", StartTime: time.Now()})
	b.AddInsightEventInternal(xray.InsightEvent{InsightID: "ins-1", Summary: "event-1", EventTime: time.Now()})

	events, err := b.GetInsightEvents("ins-1")
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "event-1", events[0].Summary)
}

func TestBackend_Insight_EndTime(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	insight := xray.Insight{
		InsightID: "end-time-test",
		GroupName: "my-group",
		GroupARN:  "arn:aws:xray:us-east-1:123456789012:group/default/my-group",
		State:     "ACTIVE",
		StartTime: time.Now(),
	}
	b.AddInsightInternal(insight)

	fetched, err := b.GetInsight("end-time-test")
	require.NoError(t, err)
	assert.True(t, fetched.EndTime.IsZero(), "EndTime should be zero when not set")
	assert.NotZero(t, fetched.StartTime)
}

func TestBackend_Insight_EndTimeSet(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	endTime := time.Now().Add(time.Hour)
	insight := xray.Insight{
		InsightID: "end-time-set-test",
		GroupName: "my-group",
		GroupARN:  "arn:aws:xray:us-east-1:123:group/default/my-group",
		State:     "CLOSED",
		StartTime: time.Now(),
		EndTime:   endTime,
	}
	b.AddInsightInternal(insight)

	fetched, err := b.GetInsight("end-time-set-test")
	require.NoError(t, err)
	assert.False(t, fetched.EndTime.IsZero())
	assert.Equal(t, endTime.Unix(), fetched.EndTime.Unix())
}
