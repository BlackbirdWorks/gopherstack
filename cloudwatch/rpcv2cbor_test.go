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

func TestCBOR_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newCBORHandler()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, cborTestServicePath+"PutMetricAlarm", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Equal(t, "PutMetricAlarm", h.ExtractOperation(c))
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

	descRec := postCBOR(t, h, "DescribeAlarms", cbor.Map{
		"AlarmNames": cbor.List{cbor.String("to-delete")},
	})
	m := decodeCBORResponse(t, descRec)
	alarms := m["MetricAlarms"].(cbor.List)
	assert.Empty(t, alarms)
}

func TestCBOR_InvalidBody(t *testing.T) {
	t.Parallel()

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
}

func TestCBOR_EmptyBody(t *testing.T) {
	t.Parallel()

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
}

func TestCBOR(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		setup            func(t *testing.T, h *cloudwatch.Handler)
		op               string
		body             cbor.Map
		wantCode         int
		wantProtocol     bool
		wantStringField  string
		wantStringValue  string
		wantListField    string
		wantListNotEmpty bool
		wantListEmpty    bool
		wantListLen      int
	}{
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
			wantCode:     http.StatusOK,
			wantProtocol: true,
		},
		{
			name:         "PutMetricData/missing namespace",
			op:           "PutMetricData",
			body:         cbor.Map{},
			wantCode:     http.StatusBadRequest,
			wantProtocol: true,
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
			wantCode:         http.StatusOK,
			wantStringField:  "Label",
			wantStringValue:  "Requests",
			wantListField:    "Datapoints",
			wantListNotEmpty: true,
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
			wantCode: http.StatusOK,
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
			wantCode:      http.StatusOK,
			wantListField: "MetricAlarms",
			wantListLen:   1,
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
			wantCode:         http.StatusOK,
			wantListField:    "Metrics",
			wantListNotEmpty: true,
		},
		{
			name:         "UnknownOperation",
			op:           "NotAnOp",
			body:         cbor.Map{},
			wantCode:     http.StatusBadRequest,
			wantProtocol: true,
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
			wantCode:         http.StatusOK,
			wantProtocol:     true,
			wantListField:    "MetricDataResults",
			wantListNotEmpty: true,
		},
		{
			name:          "GetMetricData/empty queries",
			op:            "GetMetricData",
			body:          cbor.Map{},
			wantCode:      http.StatusOK,
			wantListField: "MetricDataResults",
			wantListEmpty: true,
		},
		{
			name:         "PutMetricAlarm/missing name",
			op:           "PutMetricAlarm",
			body:         cbor.Map{},
			wantCode:     http.StatusBadRequest,
			wantProtocol: true,
		},
		{
			name:          "DescribeAlarms/empty",
			op:            "DescribeAlarms",
			body:          cbor.Map{},
			wantCode:      http.StatusOK,
			wantListField: "MetricAlarms",
			wantListEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newCBORHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postCBOR(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantProtocol {
				assert.Equal(t, "rpc-v2-cbor", rec.Header().Get("Smithy-Protocol"))
			}

			if tt.wantStringField != "" || tt.wantListField != "" {
				m := decodeCBORResponse(t, rec)

				if tt.wantStringField != "" {
					assert.Equal(t, tt.wantStringValue, string(m[tt.wantStringField].(cbor.String)))
				}

				if tt.wantListField != "" {
					list, ok := m[tt.wantListField].(cbor.List)
					require.True(t, ok)

					if tt.wantListNotEmpty {
						assert.NotEmpty(t, list)
					}

					if tt.wantListEmpty {
						assert.Empty(t, list)
					}

					if tt.wantListLen > 0 {
						assert.Len(t, list, tt.wantListLen)
					}
				}
			}
		})
	}
}
