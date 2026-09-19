package cloudwatchlogs_test

// Golden byte-equality fixtures for the FilterLogEvents/GetLogEvents perf
// sweep (gopherstack perf sweep, 2026-09-19: reflection-free stable sort +
// preallocated candidate slice in FilterLogEvents). These tests only exercise
// the public HTTP handler, so the same file runs unmodified against the
// pre-change code (a detached worktree at the commit before this sweep) to
// capture testdata/*.golden.json, and against the optimized code to assert
// byte-for-byte identical responses.
//
// To regenerate the golden files: CWL_GOLDEN_UPDATE=1 go test -run TestGolden ./services/cloudwatchlogs/...

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func goldenLogsPath(name string) string {
	return filepath.Join("testdata", name)
}

// redactVolatile walks v and blanks any field PutLogEvents always stamps
// from the wall clock (ingestionTime, and eventId which derives from it),
// before golden comparison.
func redactVolatile(v any) any {
	switch val := v.(type) {
	case map[string]any:
		for k, sub := range val {
			if k == "ingestionTime" || k == "eventId" {
				val[k] = "<redacted>"

				continue
			}
			val[k] = redactVolatile(sub)
		}

		return val
	case []any:
		for i, sub := range val {
			val[i] = redactVolatile(sub)
		}

		return val
	default:
		return v
	}
}

// checkGoldenLogs writes got to testdata/name when CWL_GOLDEN_UPDATE=1 is
// set, otherwise asserts got is byte-identical to the committed golden file
// after redactVolatile's wall-clock fields are blanked.
func checkGoldenLogs(t *testing.T, name string, got []byte) {
	t.Helper()

	var parsed any
	require.NoError(t, json.Unmarshal(got, &parsed))
	redacted, err := json.Marshal(redactVolatile(parsed))
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, json.Indent(&buf, redacted, "", "  "))
	pretty := buf.Bytes()

	path := goldenLogsPath(name)
	if os.Getenv("CWL_GOLDEN_UPDATE") != "" {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(path, pretty, 0o600))

		return
	}

	want, err := os.ReadFile(path)
	require.NoError(t, err, "golden file %s missing -- run with CWL_GOLDEN_UPDATE=1 first", path)
	require.Equal(t, string(want), string(pretty), "response for %s changed", name)
}

// seedGoldenStream seeds n events on a fresh stream. Timestamps stay below
// minRealisticTimestampMs (see store.go) so they're treated as synthetic
// fixture data and bypass PutLogEvents' wall-clock accept window and
// chronological-order checks -- keeping this fixture stable regardless of
// when the test runs, unlike a real epoch value which ages out in 14 days.
func seedGoldenStream(
	t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo, group, stream string, n int, base int64,
) {
	t.Helper()

	rec := doLogsRequest(t, h, e, "CreateLogStream", fmt.Sprintf(
		`{"logGroupName":%q,"logStreamName":%q}`, group, stream))
	require.Equal(t, http.StatusOK, rec.Code, "CreateLogStream %s: %s", stream, rec.Body.String())

	events := make([]map[string]any, n)
	for i := range n {
		events[i] = map[string]any{
			"message":   fmt.Sprintf("line %s %d", stream, i),
			"timestamp": base + int64(i),
		}
	}
	body, err := json.Marshal(map[string]any{
		"logGroupName": group, "logStreamName": stream, "logEvents": events,
	})
	require.NoError(t, err)

	rec = doLogsRequest(t, h, e, "PutLogEvents", string(body))
	require.Equal(t, http.StatusOK, rec.Code, "PutLogEvents seed %s: %s", stream, rec.Body.String())
}

// TestGolden_FilterLogEventsInterleaved seeds 3 streams with overlapping
// timestamp ranges and asserts FilterLogEvents' merged, sorted, paginated
// response is byte-identical before and after the sort/prealloc change.
func TestGolden_FilterLogEventsInterleaved(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	rec := doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"golden-grp"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	const base = int64(1000)
	seedGoldenStream(t, h, e, "golden-grp", "stream-a", 20, base)
	seedGoldenStream(t, h, e, "golden-grp", "stream-b", 20, base+5)
	seedGoldenStream(t, h, e, "golden-grp", "stream-c", 20, base+10)

	body, err := json.Marshal(map[string]any{
		"logGroupName":  "golden-grp",
		"filterPattern": "line",
		"limit":         50,
	})
	require.NoError(t, err)

	rec = doLogsRequest(t, h, e, "FilterLogEvents", string(body))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	checkGoldenLogs(t, "golden_filter_log_events.json", rec.Body.Bytes())
}

// TestGolden_GetLogEvents seeds a single stream and asserts GetLogEvents'
// response is byte-identical before and after the perf sweep.
func TestGolden_GetLogEvents(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	rec := doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"golden-grp2"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	seedGoldenStream(t, h, e, "golden-grp2", "s", 30, 1000)

	body, err := json.Marshal(map[string]any{
		"logGroupName":  "golden-grp2",
		"logStreamName": "s",
		"limit":         10,
	})
	require.NoError(t, err)

	rec = doLogsRequest(t, h, e, "GetLogEvents", string(body))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	checkGoldenLogs(t, "golden_get_log_events.json", rec.Body.Bytes())
}
