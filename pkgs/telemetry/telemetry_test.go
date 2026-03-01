package telemetry_test

import (
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/labstack/echo/v5"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errBoom is a package-level sentinel used in handler error tests (required by err113).
var errBoom = errors.New("boom")

// mockObserver is a simple ObservabilityObserver for testing WrapEchoHandler.
type mockObserver struct {
	operation string
	resource  string
}

func (m *mockObserver) ExtractOperation(_ *echo.Context) string { return m.operation }
func (m *mockObserver) ExtractResource(_ *echo.Context) string  { return m.resource }

// refiningObserver returns a different operation on the first call vs subsequent calls.
type refiningObserver struct {
	callCount  *int
	first      string
	subsequent string
	resource   string
}

func (r *refiningObserver) ExtractOperation(_ *echo.Context) string {
	*r.callCount++
	if *r.callCount == 1 {
		return r.first
	}

	return r.subsequent
}

func (r *refiningObserver) ExtractResource(_ *echo.Context) string { return r.resource }

func TestRecordOperation(t *testing.T) {
	t.Parallel()

	type recordOp struct {
		operation string
		resource  string
		latency   float64
		status    string
	}

	tests := []struct {
		name string
		ops  []recordOp
	}{
		{
			name: "success",
			ops:  []recordOp{{"TestPutItem", "TestUserTable", 0.123, "success"}},
		},
		{
			name: "error",
			ops: []recordOp{
				{"TestDeleteItem", "TestTable1", 0.050, "success"},
				{"TestDeleteItem", "TestTable1", 0.075, "error"},
			},
		},
		{
			name: "multiple_operations",
			ops: []recordOp{
				{"TestGetItem", "TestUsers", 0.010, "success"},
				{"TestGetItem", "TestUsers", 0.015, "success"},
				{"TestPutItem2", "TestUsers", 0.020, "success"},
				{"TestQuery", "TestOrders", 0.050, "success"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for _, op := range tt.ops {
				telemetry.RecordOperation(op.operation, op.resource, op.latency, op.status)
			}

			result := telemetry.CollectMetrics()
			require.NotNil(t, result)
			assert.NotNil(t, result.Operations)
		})
	}
}

func TestRecordOperation_LatencyAggregation(t *testing.T) {
	t.Parallel()

	latencies := []float64{0.010, 0.015, 0.020, 0.025, 0.030}
	for _, latency := range latencies {
		telemetry.RecordOperation("TestUpdateItem", "TestTable2", latency, "success")
	}

	result := telemetry.CollectMetrics()
	require.NotNil(t, result)

	var updateMetric *telemetry.Summary
	for i := range result.Operations {
		if result.Operations[i].Operation == "TestUpdateItem" {
			updateMetric = &result.Operations[i]

			break
		}
	}

	if updateMetric != nil {
		assert.Equal(t, int64(5), updateMetric.Count, "expected 5 operations")

		assert.Greater(t, updateMetric.AvgMs, 0.0)
		assert.Greater(t, updateMetric.MaxMs, 0.0)
		assert.Greater(t, updateMetric.P99Ms, 0.0)
		assert.Greater(t, updateMetric.P95Ms, 0.0)
		assert.Greater(t, updateMetric.P50Ms, 0.0)
		assert.LessOrEqual(t, updateMetric.P50Ms, updateMetric.MaxMs)
	}
}

func TestCollectMetrics_ValidStructure(t *testing.T) {
	t.Parallel()

	result := telemetry.CollectMetrics()
	require.NotNil(t, result)

	assert.NotNil(t, result.Operations)
	assert.IsType(t, []telemetry.Summary{}, result.Operations)
}

func TestRecordLockDuration(t *testing.T) {
	t.Parallel()

	telemetry.RecordLockDuration("TestRLock", (50 * time.Millisecond).Seconds())
	telemetry.RecordLockDuration("TestLock", (100 * time.Millisecond).Seconds())
}

func TestMetricsPrecision(t *testing.T) {
	t.Parallel()

	duration := 0.0001234
	telemetry.RecordOperation("TestScan", "TestTable3", duration, "success")

	result := telemetry.CollectMetrics()
	require.NotNil(t, result)

	var scanMetric *telemetry.Summary
	for i := range result.Operations {
		if result.Operations[i].Operation == "TestScan" {
			scanMetric = &result.Operations[i]

			break
		}
	}

	if scanMetric != nil {
		assert.Greater(t, scanMetric.AvgMs, 0.0)
		assert.Less(t, scanMetric.AvgMs, 1.0, "expected sub-millisecond operation")
	}
}

func TestCollectMetrics_RuntimeMetrics(t *testing.T) {
	t.Parallel()

	result := telemetry.CollectMetrics()
	require.NotNil(t, result)

	assert.Positive(t, result.Runtime.Goroutines, "expected at least 1 goroutine")
	assert.GreaterOrEqual(t, result.Runtime.HeapAllocMB, 0.0, "heap allocation should be >= 0")
	assert.GreaterOrEqual(t, result.Runtime.HeapSysMB, 0.0, "heap sys should be >= 0")
	assert.GreaterOrEqual(t, result.Runtime.LastGCPause, 0.0, "last GC pause should be >= 0")
	assert.GreaterOrEqual(t, result.Runtime.TotalAllocMB, 0.0, "total alloc should be >= 0")

	assert.LessOrEqual(t, result.Runtime.HeapAllocMB, result.Runtime.HeapSysMB,
		"heap alloc should not exceed heap sys")
}

func TestRecordDeleteQueueDepth(t *testing.T) {
	t.Parallel()

	telemetry.RecordDeleteQueueDepth("s3", 5)
	telemetry.RecordDeleteQueueDepth("dynamodb", 0)
}

func TestRecordTTLEvictions(t *testing.T) {
	t.Parallel()

	telemetry.RecordTTLEvictions("dynamodb", 10)
	telemetry.RecordTTLEvictions("dynamodb", 0)
}

func TestRecordStreamEvents(t *testing.T) {
	t.Parallel()

	telemetry.RecordStreamEvents("dynamodb", 3)
	telemetry.RecordStreamEvents("dynamodb", 0)
}

func TestRecordWorkerTask(t *testing.T) {
	t.Parallel()

	telemetry.RecordWorkerTask("dynamodb", "TableCleaner", "success")
	telemetry.RecordWorkerTask("s3", "BucketJanitor", "error")
}

func TestRecordWorkerItems(t *testing.T) {
	t.Parallel()

	telemetry.RecordWorkerItems("dynamodb", "TTLSweeper", 42)
	telemetry.RecordWorkerItems("dynamodb", "TTLSweeper", 0)
}

func TestRecordWorkerQueueDepth(t *testing.T) {
	t.Parallel()

	telemetry.RecordWorkerQueueDepth("s3", "BucketJanitor", 7)
	telemetry.RecordWorkerQueueDepth("s3", "BucketJanitor", 0)
}

func TestGetMetrics(t *testing.T) {
	t.Parallel()

	result := telemetry.GetMetrics()
	require.NotNil(t, result)
	_, ok := result["operations"]
	assert.True(t, ok, "expected 'operations' key in GetMetrics result")
}

func TestWrapEchoHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		service  string
		handler  func(*echo.Context) error
		observer telemetry.ObservabilityObserver
		method   string
		wantErr  error
		wantCode int
	}{
		{
			name:     "success",
			service:  "dynamodb",
			handler:  func(c *echo.Context) error { return c.String(http.StatusOK, "ok") },
			observer: &mockObserver{operation: "GetItem", resource: "MyTable"},
			method:   http.MethodGet,
			wantCode: http.StatusOK,
		},
		{
			name:     "handler_error",
			service:  "dynamodb",
			handler:  func(_ *echo.Context) error { return errBoom },
			observer: &mockObserver{operation: "PutItem", resource: "MyTable"},
			method:   http.MethodPost,
			wantErr:  errBoom,
		},
		{
			name:    "unknown_operation",
			service: "s3",
			handler: func(c *echo.Context) error { return c.String(http.StatusOK, "ok") },
			observer: &refiningObserver{
				callCount:  new(int),
				first:      "Unknown",
				subsequent: "Resolved",
				resource:   "table",
			},
			method:   http.MethodGet,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler := telemetry.WrapEchoHandler(tt.service, tt.handler, tt.observer, slog.Default())

			e := echo.New()
			req := httptest.NewRequest(tt.method, "/", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := handler(c)

			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
