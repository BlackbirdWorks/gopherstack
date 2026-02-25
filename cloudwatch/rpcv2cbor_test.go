package cloudwatch_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/cloudwatch"
)

const cborTestServicePath = "/service/GraniteServiceVersion20100801/operation/"

// postCBOR sends a rpc-v2-cbor POST to the CloudWatch handler.
func postCBOR(t *testing.T, h *cloudwatch.Handler, op string, body cbor.Map) *httptest.ResponseRecorder {
	t.Helper()

	encoded := cbor.Encode(body)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, cborTestServicePath+op, bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/cbor")
	req.Header.Set("Smithy-Protocol", "rpc-v2-cbor")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

func newCBORHandler() *cloudwatch.Handler {
	return cloudwatch.NewHandler(cloudwatch.NewInMemoryBackend(), slog.Default())
}

// decodeCBORResponse decodes the CBOR response body into a cbor.Map.
func decodeCBORResponse(t *testing.T, rec *httptest.ResponseRecorder) cbor.Map {
	t.Helper()

	v, err := cbor.Decode(rec.Body.Bytes())
	require.NoError(t, err)

	m, ok := v.(cbor.Map)
	require.True(t, ok, "expected CBOR map response")

	return m
}

func TestCBOR_RouteMatcher_MatchesCBOR(t *testing.T) {
	t.Parallel()

	h := newCBORHandler()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, cborTestServicePath+"PutMetricData", nil)
	req.Header.Set("Content-Type", "application/cbor")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.True(t, h.RouteMatcher()(c))
}

func TestCBOR_RouteMatcher_RejectsUnknownOp(t *testing.T) {
	t.Parallel()

	h := newCBORHandler()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, cborTestServicePath+"UnknownOp", nil)
	req.Header.Set("Content-Type", "application/cbor")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.False(t, h.RouteMatcher()(c))
}

func TestCBOR_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newCBORHandler()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, cborTestServicePath+"PutMetricAlarm", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Equal(t, "PutMetricAlarm", h.ExtractOperation(c))
}

func TestCBOR_PutMetricData(t *testing.T) {
	t.Parallel()

	h := newCBORHandler()
	rec := postCBOR(t, h, "PutMetricData", cbor.Map{
		"Namespace": cbor.String("TestNS"),
		"MetricData": cbor.List{
			cbor.Map{
				"MetricName": cbor.String("Latency"),
				"Value":      cbor.Float64(123.0),
				"Timestamp":  cbor.Tag{ID: 1, Value: cbor.Float64(float64(time.Now().Unix()))},
			},
		},
	})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "rpc-v2-cbor", rec.Header().Get("Smithy-Protocol"))
}

func TestCBOR_PutMetricData_MissingNamespace(t *testing.T) {
	t.Parallel()

	h := newCBORHandler()
	rec := postCBOR(t, h, "PutMetricData", cbor.Map{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "rpc-v2-cbor", rec.Header().Get("Smithy-Protocol"))
}

func TestCBOR_PutAndGetMetricStatistics(t *testing.T) {
	t.Parallel()

	h := newCBORHandler()
	ts := time.Now().UTC()

	// Put metric data
	putRec := postCBOR(t, h, "PutMetricData", cbor.Map{
		"Namespace": cbor.String("StatNS"),
		"MetricData": cbor.List{
			cbor.Map{
				"MetricName": cbor.String("Requests"),
				"Value":      cbor.Float64(50.0),
				"Timestamp":  cbor.Tag{ID: 1, Value: cbor.Float64(float64(ts.Unix()))},
			},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	// Get stats
	rec := postCBOR(t, h, "GetMetricStatistics", cbor.Map{
		"Namespace":  cbor.String("StatNS"),
		"MetricName": cbor.String("Requests"),
		"StartTime":  cbor.Tag{ID: 1, Value: cbor.Float64(float64(ts.Add(-time.Hour).Unix()))},
		"EndTime":    cbor.Tag{ID: 1, Value: cbor.Float64(float64(ts.Add(time.Minute).Unix()))},
		"Period":     cbor.Uint(3600),
		"Statistics": cbor.List{cbor.String("Sum")},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	m := decodeCBORResponse(t, rec)
	assert.Equal(t, "Requests", string(m["Label"].(cbor.String)))

	dps, ok := m["Datapoints"].(cbor.List)
	require.True(t, ok)
	assert.NotEmpty(t, dps)
}

func TestCBOR_PutMetricAlarm(t *testing.T) {
	t.Parallel()

	h := newCBORHandler()
	rec := postCBOR(t, h, "PutMetricAlarm", cbor.Map{
		"AlarmName":          cbor.String("test-alarm"),
		"Namespace":          cbor.String("TestNS"),
		"MetricName":         cbor.String("Errors"),
		"ComparisonOperator": cbor.String("GreaterThanThreshold"),
		"Statistic":          cbor.String("Sum"),
		"Threshold":          cbor.Float64(10.0),
		"EvaluationPeriods":  cbor.Uint(1),
		"Period":             cbor.Uint(60),
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestCBOR_DescribeAlarms(t *testing.T) {
	t.Parallel()

	h := newCBORHandler()

	// Create alarm
	postCBOR(t, h, "PutMetricAlarm", cbor.Map{
		"AlarmName":          cbor.String("my-alarm"),
		"Namespace":          cbor.String("NS"),
		"MetricName":         cbor.String("M"),
		"ComparisonOperator": cbor.String("GreaterThanThreshold"),
		"Threshold":          cbor.Float64(5.0),
		"EvaluationPeriods":  cbor.Uint(1),
		"Period":             cbor.Uint(60),
	})

	// Describe
	rec := postCBOR(t, h, "DescribeAlarms", cbor.Map{
		"AlarmNames": cbor.List{cbor.String("my-alarm")},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	m := decodeCBORResponse(t, rec)
	alarms, ok := m["MetricAlarms"].(cbor.List)
	require.True(t, ok)
	assert.Len(t, alarms, 1)
}

func TestCBOR_DeleteAlarms(t *testing.T) {
	t.Parallel()

	h := newCBORHandler()

	postCBOR(t, h, "PutMetricAlarm", cbor.Map{
		"AlarmName":          cbor.String("to-delete"),
		"ComparisonOperator": cbor.String("GreaterThanThreshold"),
		"Threshold":          cbor.Float64(1.0),
		"EvaluationPeriods":  cbor.Uint(1),
		"Period":             cbor.Uint(60),
	})

	rec := postCBOR(t, h, "DeleteAlarms", cbor.Map{
		"AlarmNames": cbor.List{cbor.String("to-delete")},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	// Verify gone
	descRec := postCBOR(t, h, "DescribeAlarms", cbor.Map{
		"AlarmNames": cbor.List{cbor.String("to-delete")},
	})

	m := decodeCBORResponse(t, descRec)
	alarms := m["MetricAlarms"].(cbor.List)
	assert.Empty(t, alarms)
}

func TestCBOR_ListMetrics(t *testing.T) {
	t.Parallel()

	h := newCBORHandler()

	postCBOR(t, h, "PutMetricData", cbor.Map{
		"Namespace": cbor.String("ListNS"),
		"MetricData": cbor.List{
			cbor.Map{
				"MetricName": cbor.String("CPU"),
				"Value":      cbor.Float64(80.0),
				"Timestamp":  cbor.Tag{ID: 1, Value: cbor.Float64(float64(time.Now().Unix()))},
			},
		},
	})

	rec := postCBOR(t, h, "ListMetrics", cbor.Map{
		"Namespace": cbor.String("ListNS"),
	})

	require.Equal(t, http.StatusOK, rec.Code)

	m := decodeCBORResponse(t, rec)
	metrics, ok := m["Metrics"].(cbor.List)
	require.True(t, ok)
	assert.NotEmpty(t, metrics)
}

func TestCBOR_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newCBORHandler()
	rec := postCBOR(t, h, "NotAnOp", cbor.Map{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "rpc-v2-cbor", rec.Header().Get("Smithy-Protocol"))
}

func TestCBOR_InvalidBody(t *testing.T) {
	t.Parallel()

	h := newCBORHandler()
	e := echo.New()
	req := httptest.NewRequest(
		http.MethodPost,
		cborTestServicePath+"PutMetricData",
		bytes.NewReader([]byte{0x00, 0xFF, 0xAA}), // invalid CBOR
	)
	req.Header.Set("Content-Type", "application/cbor")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
