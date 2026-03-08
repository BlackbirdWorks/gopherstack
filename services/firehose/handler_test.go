package firehose_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/firehose"
)

func newTestFirehoseHandler(t *testing.T) *firehose.Handler {
	t.Helper()

	return firehose.NewHandler(firehose.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doFirehoseRequest(t *testing.T, h *firehose.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Firehose_20150804."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestFirehoseHandler_CreateDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *firehose.Handler)
		name         string
		streamName   string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			streamName:   "my-stream",
			wantCode:     http.StatusOK,
			wantContains: []string{"arn:aws:firehose:"},
		},
		{
			name:       "already_exists",
			streamName: "my-stream",
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{
				"DeliveryStreamName": tt.streamName,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestFirehoseHandler_DeleteDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *firehose.Handler)
		streamName string
		wantCode   int
	}{
		{
			name:       "success",
			streamName: "my-stream",
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
			},
			wantCode: http.StatusOK,
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doFirehoseRequest(t, h, "DeleteDeliveryStream", map[string]any{
				"DeliveryStreamName": tt.streamName,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestFirehoseHandler_DescribeDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *firehose.Handler)
		name         string
		streamName   string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "success",
			streamName: "my-stream",
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DeliveryStreamDescription"},
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doFirehoseRequest(t, h, "DescribeDeliveryStream", map[string]any{
				"DeliveryStreamName": tt.streamName,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestFirehoseHandler_ListDeliveryStreams(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "s1"})
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "s2"})

	rec := doFirehoseRequest(t, h, "ListDeliveryStreams", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeliveryStreamNames")
}

func TestFirehoseHandler_PutRecord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *firehose.Handler)
		streamName string
		data       string
		wantCode   int
	}{
		{
			name:       "success",
			streamName: "my-stream",
			data:       base64.StdEncoding.EncodeToString([]byte("hello world")),
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
			},
			wantCode: http.StatusOK,
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			data:       base64.StdEncoding.EncodeToString([]byte("hello")),
			wantCode:   http.StatusNotFound,
		},
		{
			name:       "raw_data",
			streamName: "my-stream",
			data:       "not-base64!@#",
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
				"DeliveryStreamName": tt.streamName,
				"Record":             map[string]string{"Data": tt.data},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestFirehoseHandler_PutRecordBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *firehose.Handler)
		streamName string
		data       string
		wantCode   int
	}{
		{
			name:       "success",
			streamName: "my-stream",
			data:       base64.StdEncoding.EncodeToString([]byte("rec1")),
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
			},
			wantCode: http.StatusOK,
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			data:       base64.StdEncoding.EncodeToString([]byte("a")),
			wantCode:   http.StatusNotFound,
		},
		{
			name:       "raw_data",
			streamName: "my-stream",
			data:       "not-base64!@#",
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doFirehoseRequest(t, h, "PutRecordBatch", map[string]any{
				"DeliveryStreamName": tt.streamName,
				"Records":            []map[string]string{{"Data": tt.data}},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestFirehoseHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "UnknownAction", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestFirehoseHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "match",
			target: "Firehose_20150804.CreateDeliveryStream",
			want:   true,
		},
		{
			name:   "no_match",
			target: "SimpleWorkflowService.RegisterDomain",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestFirehoseHandler_ProviderName(t *testing.T) {
	t.Parallel()

	p := &firehose.Provider{}
	assert.Equal(t, "Firehose", p.Name())
}

func TestFirehoseHandler_HandlerName(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	assert.Equal(t, "Firehose", h.Name())
}

func TestFirehoseHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateDeliveryStream")
	assert.Contains(t, ops, "DeleteDeliveryStream")
	assert.Contains(t, ops, "DescribeDeliveryStream")
	assert.Contains(t, ops, "ListDeliveryStreams")
	assert.Contains(t, ops, "PutRecord")
	assert.Contains(t, ops, "PutRecordBatch")
}

func TestFirehoseHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestFirehoseHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "with_target",
			target: "Firehose_20150804.CreateDeliveryStream",
			want:   "CreateDeliveryStream",
		},
		{
			name: "no_target",
			want: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestFirehoseHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"DeliveryStreamName":"my-stream"}`))
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "my-stream", h.ExtractResource(c))
}

func TestFirehoseHandler_TagDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *firehose.Handler)
		streamName string
		tags       []map[string]string
		wantCode   int
	}{
		{
			name:       "success",
			streamName: "my-stream",
			tags:       []map[string]string{{"Key": "env", "Value": "prod"}},
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
			},
			wantCode: http.StatusOK,
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			tags:       []map[string]string{{"Key": "env", "Value": "prod"}},
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
				"DeliveryStreamName": tt.streamName,
				"Tags":               tt.tags,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestFirehoseHandler_UntagDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *firehose.Handler)
		streamName string
		tagKeys    []string
		wantCode   int
	}{
		{
			name:       "success",
			streamName: "my-stream",
			tagKeys:    []string{"env"},
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
				doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
					"DeliveryStreamName": "my-stream",
					"Tags":               []map[string]string{{"Key": "env", "Value": "prod"}},
				})
			},
			wantCode: http.StatusOK,
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			tagKeys:    []string{"env"},
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doFirehoseRequest(t, h, "UntagDeliveryStream", map[string]any{
				"DeliveryStreamName": tt.streamName,
				"TagKeys":            tt.tagKeys,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestFirehoseHandler_ListTagsForDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *firehose.Handler)
		name         string
		streamName   string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "empty_tags",
			streamName: "my-stream",
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"Tags", "HasMoreTags"},
		},
		{
			name:       "with_tags",
			streamName: "my-stream",
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
				doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
					"DeliveryStreamName": "my-stream",
					"Tags":               []map[string]string{{"Key": "env", "Value": "prod"}},
				})
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"env", "prod"},
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doFirehoseRequest(t, h, "ListTagsForDeliveryStream", map[string]any{
				"DeliveryStreamName": tt.streamName,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestFirehoseHandler_ProviderInit(t *testing.T) {
	t.Parallel()

	p := &firehose.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "Firehose", svc.Name())
}

func TestFirehoseHandler_CreateDeliveryStream_WithS3Destination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "s3_destination",
			body: map[string]any{
				"DeliveryStreamName": "s3-stream",
				"S3DestinationConfiguration": map[string]any{
					"BucketARN": "arn:aws:s3:::my-bucket",
					"RoleARN":   "arn:aws:iam::000000000000:role/firehose",
					"BufferingHints": map[string]any{
						"SizeInMBs":         5,
						"IntervalInSeconds": 300,
					},
					"CompressionFormat": "GZIP",
				},
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DeliveryStreamARN"},
		},
		{
			name: "extended_s3_destination",
			body: map[string]any{
				"DeliveryStreamName": "ext-s3-stream",
				"ExtendedS3DestinationConfiguration": map[string]any{
					"BucketARN": "arn:aws:s3:::ext-bucket",
					"RoleARN":   "arn:aws:iam::000000000000:role/firehose",
					"ProcessingConfiguration": map[string]any{
						"Enabled": true,
						"Processors": []map[string]any{
							{
								"Type": "Lambda",
								"Parameters": []map[string]any{
									{
										"ParameterName":  "LambdaArn",
										"ParameterValue": "my-fn",
									},
								},
							},
						},
					},
				},
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DeliveryStreamARN"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			rec := doFirehoseRequest(t, h, "CreateDeliveryStream", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestFirehoseHandler_DescribeDeliveryStream_WithS3Destination(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "describe-s3-stream",
		"S3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::my-bucket",
		},
	})

	rec := doFirehoseRequest(t, h, "DescribeDeliveryStream", map[string]any{
		"DeliveryStreamName": "describe-s3-stream",
	})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "S3DestinationDescriptions")
	assert.Contains(t, rec.Body.String(), "arn:aws:s3:::my-bucket")
}

func TestFirehoseHandler_UpdateDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *firehose.Handler)
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{
					"DeliveryStreamName": "upd-stream",
				})
			},
			body: map[string]any{
				"DeliveryStreamName":             "upd-stream",
				"CurrentDeliveryStreamVersionId": "1",
				"DestinationId":                  "destinationId-000000000001",
				"S3DestinationUpdate": map[string]any{
					"BucketARN": "arn:aws:s3:::new-bucket",
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "not_found",
			body: map[string]any{
				"DeliveryStreamName": "nonexistent",
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doFirehoseRequest(t, h, "UpdateDestination", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
