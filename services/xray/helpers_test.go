package xray_test

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

	"github.com/blackbirdworks/gopherstack/services/xray"
)

func newTestBackend(t *testing.T) *xray.InMemoryBackend {
	t.Helper()

	return xray.NewInMemoryBackend("000000000000", "us-east-1")
}

// buildFaultSegmentJSON returns a raw segment JSON string for a named service.
func buildFaultSegmentJSON(t *testing.T, traceID, name string, fault bool) string {
	t.Helper()

	seg := map[string]any{
		"trace_id":   traceID,
		"id":         "seg-" + name,
		"name":       name,
		"start_time": 1.0,
		"end_time":   1.1,
		"fault":      fault,
	}

	data, err := json.Marshal(seg)
	require.NoError(t, err)

	return string(data)
}

func newTestHandler(t *testing.T) *xray.Handler {
	t.Helper()

	return xray.NewHandler(xray.NewInMemoryBackend("000000000000", "us-east-1"))
}

func newTestHandlerWithBackend(t *testing.T) (*xray.Handler, *xray.InMemoryBackend) {
	t.Helper()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")

	return xray.NewHandler(b), b
}

func doXrayRequest(t *testing.T, h *xray.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.RequestURI = path

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func doXrayGETRequest(t *testing.T, h *xray.Handler) *httptest.ResponseRecorder {
	t.Helper()

	const path = "/EncryptionConfig"

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.RequestURI = path

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// segJSON builds a minimal segment JSON string.
func segJSON(traceID, segID, parentID, name string, startTime, endTime float64, fault, hasError, throttle bool) string {
	d := fmt.Sprintf(
		`{"trace_id":%q,"id":%q,"name":%q,"start_time":%f,"end_time":%f,"fault":%v,"error":%v,"throttle":%v`,
		traceID, segID, name, startTime, endTime, fault, hasError, throttle,
	)

	if parentID != "" {
		d += fmt.Sprintf(`,"parent_id":%q`, parentID)
	}

	d += "}"

	return d
}

// segJSONWithHTTP builds a segment with HTTP fields.
func segJSONWithHTTP(traceID, segID, name string, startTime, endTime float64, status int, method, url string) string {
	return fmt.Sprintf(
		`{"trace_id":%q,"id":%q,"name":%q,"start_time":%f,"end_time":%f,`+
			`"http":{"request":{"method":%q,"url":%q},"response":{"status":%d}}}`,
		traceID, segID, name, startTime, endTime, method, url, status,
	)
}

// segFmt formats a float64 timestamp as a JSON value string (no quotes).
func segFmt(ts float64) string {
	out, _ := json.Marshal(ts)

	return string(out)
}

// seedInsight adds an insight to the backend for testing.
func seedInsight(b *xray.InMemoryBackend, id, groupName, groupARN string) {
	b.AddInsightInternal(xray.Insight{
		InsightID: id,
		GroupName: groupName,
		GroupARN:  groupARN,
		State:     "ACTIVE",
		StartTime: time.Now(),
	})
}

// seedInsightEvent adds an event for an insight.
func seedInsightEvent(b *xray.InMemoryBackend, insightID, summary string) {
	b.AddInsightEventInternal(xray.InsightEvent{
		InsightID: insightID,
		Summary:   summary,
		EventTime: time.Now(),
	})
}
