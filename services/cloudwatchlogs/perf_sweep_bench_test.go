package cloudwatchlogs_test

// Benchmarks for the gopherstack perf sweep targeting FilterLogEvents
// (2026-09-19), run through the full HTTP handler (matching production
// request path: JSON decode -> dispatch -> backend -> JSON encode).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

const logsTargetBench = "Logs_20140328."

// invokeLogsB sends action to h through the full HTTP handler and decodes
// the JSON response, for use from both benchmarks (testing.B) and setup
// helpers shared with them (testing.TB).
func invokeLogsB(
	tb testing.TB, h *cloudwatchlogs.Handler, e *echo.Echo, action string, body any,
) (int, map[string]any) {
	tb.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(tb, err)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("X-Amz-Target", logsTargetBench+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(tb, h.Handler()(c))

	var resp map[string]any
	if rec.Body.Len() > 0 {
		require.NoError(tb, json.Unmarshal(rec.Body.Bytes(), &resp))
	}

	return rec.Code, resp
}

func seedBenchStream(
	tb testing.TB, h *cloudwatchlogs.Handler, e *echo.Echo, group, stream string, n int, base int64,
) {
	tb.Helper()

	code, resp := invokeLogsB(tb, h, e, "CreateLogStream", map[string]any{
		"logGroupName":  group,
		"logStreamName": stream,
	})
	require.Equal(tb, http.StatusOK, code, "CreateLogStream %s: %v", stream, resp)

	events := make([]map[string]any, n)
	for i := range n {
		events[i] = map[string]any{
			"message":   fmt.Sprintf("line %s %d", stream, i),
			"timestamp": base + int64(i),
		}
	}

	code, resp = invokeLogsB(tb, h, e, "PutLogEvents", map[string]any{
		"logGroupName":  group,
		"logStreamName": stream,
		"logEvents":     events,
	})
	require.Equal(tb, http.StatusOK, code, "PutLogEvents seed %s: %v", stream, resp)
}

// BenchmarkPutLogEvents_100 runs PutLogEvents with a 100-event batch, through
// the full HTTP handler.
func BenchmarkPutLogEvents_100(b *testing.B) {
	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	code, resp := invokeLogsB(b, h, e, "CreateLogGroup", map[string]any{"logGroupName": "grp"})
	require.Equal(b, http.StatusOK, code, "CreateLogGroup: %v", resp)
	code, resp = invokeLogsB(b, h, e, "CreateLogStream", map[string]any{
		"logGroupName": "grp", "logStreamName": "s",
	})
	require.Equal(b, http.StatusOK, code, "CreateLogStream: %v", resp)

	base := time.Now().UnixMilli()
	events := make([]map[string]any, 100)
	for i := range events {
		events[i] = map[string]any{"message": fmt.Sprintf("line %d", i), "timestamp": base + int64(i)}
	}
	req := map[string]any{"logGroupName": "grp", "logStreamName": "s", "logEvents": events}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		loopCode, loopResp := invokeLogsB(b, h, e, "PutLogEvents", req)
		if loopCode != http.StatusOK {
			b.Fatalf("PutLogEvents failed: %v", loopResp)
		}
	}
}

// BenchmarkFilterLogEvents_10kEvents runs FilterLogEvents across 10 streams
// holding 1,000 events each (10,000 total), through the full HTTP handler.
// This is the profiled hot path: FilterLogEvents interleaves and
// timestamp-sorts every matching event on every call.
func BenchmarkFilterLogEvents_10kEvents(b *testing.B) {
	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	code, resp := invokeLogsB(b, h, e, "CreateLogGroup", map[string]any{"logGroupName": "grp"})
	require.Equal(b, http.StatusOK, code, "CreateLogGroup: %v", resp)

	const streams = 10
	const perStream = 1000
	base := time.Now().UnixMilli() - int64(perStream)
	for s := range streams {
		// Offset each stream's timestamps so the merge/sort is non-trivial
		// (streams are not already interleaved in timestamp order).
		seedBenchStream(b, h, e, "grp", fmt.Sprintf("stream-%02d", s), perStream, base+int64(s))
	}

	req := map[string]any{"logGroupName": "grp", "limit": 10000}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		loopCode, loopResp := invokeLogsB(b, h, e, "FilterLogEvents", req)
		if loopCode != http.StatusOK {
			b.Fatalf("FilterLogEvents failed: %v", loopResp)
		}
	}
}

// BenchmarkGetLogEvents_1k runs GetLogEvents against a single stream holding
// 1,000 events, through the full HTTP handler.
func BenchmarkGetLogEvents_1k(b *testing.B) {
	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	code, resp := invokeLogsB(b, h, e, "CreateLogGroup", map[string]any{"logGroupName": "grp"})
	require.Equal(b, http.StatusOK, code, "CreateLogGroup: %v", resp)

	base := time.Now().UnixMilli() - 1000
	seedBenchStream(b, h, e, "grp", "s", 1000, base)

	req := map[string]any{"logGroupName": "grp", "logStreamName": "s", "limit": 1000}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		loopCode, loopResp := invokeLogsB(b, h, e, "GetLogEvents", req)
		if loopCode != http.StatusOK {
			b.Fatalf("GetLogEvents failed: %v", loopResp)
		}
	}
}
