package cloudwatch_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/cloudwatch"
)

const cborTestServicePath = "/service/GraniteServiceVersion20100801/operation/"

// fixedTS is 2024-06-01 12:00:00 UTC as a Unix timestamp.
const fixedTS = 1717243200.0

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

func TestCBOR_RouteMatcher(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		path string
		want bool
	}{
		{
			name: "matches CBOR",
			path: cborTestServicePath + "PutMetricData",
			want: true,
		},
		{
			name: "rejects unknown op",
			path: cborTestServicePath + "UnknownOp",
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newCBORHandler()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			req.Header.Set("Content-Type", "application/cbor")
			assert.Equal(t, tt.want, h.RouteMatcher()(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func TestCBOR(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func(t *testing.T, h *cloudwatch.Handler)
		op    string
		body  cbor.Map
		want  func(t *testing.T, rec *httptest.ResponseRecorder)
		run   func(t *testing.T)
	}{
		{
			name: "ExtractOperation",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, cborTestServicePath+"PutMetricAlarm", nil)
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)
				assert.Equal(t, "PutMetricAlarm", h.ExtractOperation(c))
			},
		},
		{
			name: "PutMetricData",
			op:   "PutMetricData",
			body: cbor.Map{
				"Namespace": cbor.String("TestNS"),
				"MetricData": cbor.List{
					cbor.Map{
						"MetricName": cbor.String("Latency"),
						"Value":      cbor.Float64(123.0),
						"Timestamp":  cbor.Tag{ID: 1, Value: cbor.Float64(fixedTS)},
					},
				},
			},
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, "rpc-v2-cbor", rec.Header().Get("Smithy-Protocol"))
			},
		},
		{
			name: "PutMetricData/missing namespace",
			op:   "PutMetricData",
			body: cbor.Map{},
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Equal(t, "rpc-v2-cbor", rec.Header().Get("Smithy-Protocol"))
			},
		},
		{
			name: "PutAndGetMetricStatistics",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				putRec := postCBOR(t, h, "PutMetricData", cbor.Map{
					"Namespace": cbor.String("StatNS"),
					"MetricData": cbor.List{
						cbor.Map{
							"MetricName": cbor.String("Requests"),
							"Value":      cbor.Float64(50.0),
							"Timestamp":  cbor.Tag{ID: 1, Value: cbor.Float64(fixedTS)},
						},
					},
				})
				require.Equal(t, http.StatusOK, putRec.Code)
			},
			op: "GetMetricStatistics",
			body: cbor.Map{
				"Namespace":  cbor.String("StatNS"),
				"MetricName": cbor.String("Requests"),
				"StartTime":  cbor.Tag{ID: 1, Value: cbor.Float64(fixedTS - 3600)},
				"EndTime":    cbor.Tag{ID: 1, Value: cbor.Float64(fixedTS + 60)},
				"Period":     cbor.Uint(3600),
				"Statistics": cbor.List{cbor.String("Sum")},
			},
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeCBORResponse(t, rec)
				assert.Equal(t, "Requests", string(m["Label"].(cbor.String)))
				dps, ok := m["Datapoints"].(cbor.List)
				require.True(t, ok)
				assert.NotEmpty(t, dps)
			},
		},
		{
			name: "PutMetricAlarm",
			op:   "PutMetricAlarm",
			body: cbor.Map{
				"AlarmName":          cbor.String("test-alarm"),
				"Namespace":          cbor.String("TestNS"),
				"MetricName":         cbor.String("Errors"),
				"ComparisonOperator": cbor.String("GreaterThanThreshold"),
				"Statistic":          cbor.String("Sum"),
				"Threshold":          cbor.Float64(10.0),
				"EvaluationPeriods":  cbor.Uint(1),
				"Period":             cbor.Uint(60),
			},
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "DescribeAlarms",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				postCBOR(t, h, "PutMetricAlarm", cbor.Map{
					"AlarmName":          cbor.String("my-alarm"),
					"Namespace":          cbor.String("NS"),
					"MetricName":         cbor.String("M"),
					"ComparisonOperator": cbor.String("GreaterThanThreshold"),
					"Threshold":          cbor.Float64(5.0),
					"EvaluationPeriods":  cbor.Uint(1),
					"Period":             cbor.Uint(60),
				})
			},
			op: "DescribeAlarms",
			body: cbor.Map{
				"AlarmNames": cbor.List{cbor.String("my-alarm")},
			},
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeCBORResponse(t, rec)
				alarms, ok := m["MetricAlarms"].(cbor.List)
				require.True(t, ok)
				assert.Len(t, alarms, 1)
			},
		},
		{
			name: "DeleteAlarms",
			run: func(t *testing.T) {
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

				descRec := postCBOR(t, h, "DescribeAlarms", cbor.Map{
					"AlarmNames": cbor.List{cbor.String("to-delete")},
				})
				m := decodeCBORResponse(t, descRec)
				alarms := m["MetricAlarms"].(cbor.List)
				assert.Empty(t, alarms)
			},
		},
		{
			name: "ListMetrics",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				postCBOR(t, h, "PutMetricData", cbor.Map{
					"Namespace": cbor.String("ListNS"),
					"MetricData": cbor.List{
						cbor.Map{
							"MetricName": cbor.String("CPU"),
							"Value":      cbor.Float64(80.0),
							"Timestamp":  cbor.Tag{ID: 1, Value: cbor.Float64(fixedTS)},
						},
					},
				})
			},
			op: "ListMetrics",
			body: cbor.Map{
				"Namespace": cbor.String("ListNS"),
			},
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeCBORResponse(t, rec)
				metrics, ok := m["Metrics"].(cbor.List)
				require.True(t, ok)
				assert.NotEmpty(t, metrics)
			},
		},
		{
			name: "UnknownOperation",
			op:   "NotAnOp",
			body: cbor.Map{},
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Equal(t, "rpc-v2-cbor", rec.Header().Get("Smithy-Protocol"))
			},
		},
		{
			name: "InvalidBody",
			run: func(t *testing.T) {
				h := newCBORHandler()
				e := echo.New()
				req := httptest.NewRequest(
					http.MethodPost,
					cborTestServicePath+"PutMetricData",
					bytes.NewReader([]byte{0x00, 0xFF, 0xAA}),
				)
				req.Header.Set("Content-Type", "application/cbor")
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)

				require.NoError(t, h.Handler()(c))
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "GetMetricData",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				postCBOR(t, h, "PutMetricData", cbor.Map{
					"Namespace": cbor.String("MDataNS"),
					"MetricData": cbor.List{
						cbor.Map{
							"MetricName": cbor.String("Errors"),
							"Value":      cbor.Float64(42.0),
							"Timestamp":  cbor.Tag{ID: 1, Value: cbor.Float64(fixedTS)},
						},
					},
				})
			},
			op: "GetMetricData",
			body: cbor.Map{
				"StartTime": cbor.Tag{ID: 1, Value: cbor.Float64(fixedTS - 3600)},
				"EndTime":   cbor.Tag{ID: 1, Value: cbor.Float64(fixedTS + 60)},
				"MetricDataQueries": cbor.List{
					cbor.Map{
						"Id":    cbor.String("q1"),
						"Label": cbor.String("ErrorCount"),
						"MetricStat": cbor.Map{
							"Stat":   cbor.String("Sum"),
							"Period": cbor.Uint(3600),
							"Metric": cbor.Map{
								"Namespace":  cbor.String("MDataNS"),
								"MetricName": cbor.String("Errors"),
							},
						},
					},
				},
			},
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, "rpc-v2-cbor", rec.Header().Get("Smithy-Protocol"))
				m := decodeCBORResponse(t, rec)
				results, ok := m["MetricDataResults"].(cbor.List)
				require.True(t, ok)
				assert.NotEmpty(t, results)
			},
		},
		{
			name: "GetMetricData/empty queries",
			op:   "GetMetricData",
			body: cbor.Map{},
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusOK, rec.Code)
				m := decodeCBORResponse(t, rec)
				results, ok := m["MetricDataResults"].(cbor.List)
				require.True(t, ok)
				assert.Empty(t, results)
			},
		},
		{
			name: "PutMetricAlarm/missing name",
			op:   "PutMetricAlarm",
			body: cbor.Map{},
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Equal(t, "rpc-v2-cbor", rec.Header().Get("Smithy-Protocol"))
			},
		},
		{
			name: "DescribeAlarms/empty",
			op:   "DescribeAlarms",
			body: cbor.Map{},
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeCBORResponse(t, rec)
				alarms, ok := m["MetricAlarms"].(cbor.List)
				require.True(t, ok)
				assert.Empty(t, alarms)
			},
		},
		{
			name: "EmptyBody",
			run: func(t *testing.T) {
				h := newCBORHandler()
				e := echo.New()
				req := httptest.NewRequest(
					http.MethodPost,
					cborTestServicePath+"PutMetricData",
					bytes.NewReader(nil),
				)
				req.Header.Set("Content-Type", "application/cbor")
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)

				require.NoError(t, h.Handler()(c))
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.run != nil {
				tt.run(t)
				return
			}
			h := newCBORHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}
			if tt.op != "" {
				rec := postCBOR(t, h, tt.op, tt.body)
				if tt.want != nil {
					tt.want(t, rec)
				}
			}
		})
	}
}
