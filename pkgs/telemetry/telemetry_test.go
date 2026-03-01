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

func TestTelemetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "RecordOperation_Success", run: func(t *testing.T) {
			// Record an operation
			telemetry.RecordOperation("TestPutItem", "TestUserTable", 0.123, "success")

			// Collect metrics
			result := telemetry.CollectMetrics()
			require.NotNil(t, result)

			// Check that we have a valid structure
			assert.NotNil(t, result.Operations)
		}},
		{name: "RecordOperation_Error", run: func(t *testing.T) {
			// Record successful and failed operations with unique names
			telemetry.RecordOperation("TestDeleteItem", "TestTable1", 0.050, "success")
			telemetry.RecordOperation("TestDeleteItem", "TestTable1", 0.075, "error")

			// Collect metrics
			result := telemetry.CollectMetrics()
			require.NotNil(t, result)
			assert.NotNil(t, result.Operations)
		}},
		{name: "RecordOperation_MultipleOperations", run: func(t *testing.T) {
			// Record multiple operations with unique names
			telemetry.RecordOperation("TestGetItem", "TestUsers", 0.010, "success")
			telemetry.RecordOperation("TestGetItem", "TestUsers", 0.015, "success")
			telemetry.RecordOperation("TestPutItem2", "TestUsers", 0.020, "success")
			telemetry.RecordOperation("TestQuery", "TestOrders", 0.050, "success")

			// Collect metrics
			result := telemetry.CollectMetrics()
			require.NotNil(t, result)

			// Check that operations are there (they may or may not be depending on previous runs)
			assert.NotNil(t, result.Operations)
		}},
		{name: "RecordOperation_LatencyAggregation", run: func(t *testing.T) {
			// Record multiple latencies with unique operation name
			latencies := []float64{0.010, 0.015, 0.020, 0.025, 0.030}
			for _, latency := range latencies {
				telemetry.RecordOperation("TestUpdateItem", "TestTable2", latency, "success")
			}

			// Collect metrics
			result := telemetry.CollectMetrics()
			require.NotNil(t, result)

			// Find the TestUpdateItem metric
			var updateMetric *telemetry.Summary
			for i := range result.Operations {
				if result.Operations[i].Operation == "TestUpdateItem" {
					updateMetric = &result.Operations[i]

					break
				}
			}

			if updateMetric != nil {
				assert.Equal(t, int64(5), updateMetric.Count, "expected 5 operations")

				// Verify latencies are aggregated (in milliseconds)
				assert.Greater(t, updateMetric.AvgMs, 0.0)
				assert.Greater(t, updateMetric.MaxMs, 0.0)
				assert.Greater(t, updateMetric.P99Ms, 0.0)
				assert.Greater(t, updateMetric.P95Ms, 0.0)
				assert.Greater(t, updateMetric.P50Ms, 0.0)
				// P50 should be roughly in the middle of the range
				assert.LessOrEqual(t, updateMetric.P50Ms, updateMetric.MaxMs)
			}
		}},
		{name: "CollectMetrics_ValidStructure", run: func(t *testing.T) {
			// Collect metrics
			result := telemetry.CollectMetrics()
			require.NotNil(t, result)

			// Should always return a valid structure
			assert.NotNil(t, result.Operations)
			assert.IsType(t, []telemetry.Summary{}, result.Operations)
		}},
		{name: "RecordLockDuration", run: func(t *testing.T) {
			// Record a lock duration - should not panic
			telemetry.RecordLockDuration("TestRLock", (50 * time.Millisecond).Seconds())
			telemetry.RecordLockDuration("TestLock", (100 * time.Millisecond).Seconds())

			// Just ensure it doesn't panic; lock duration has a separate metric
		}},
		{name: "MetricsPrecision", run: func(t *testing.T) {
			// Record with high precision duration
			duration := 0.0001234
			telemetry.RecordOperation("TestScan", "TestTable3", duration, "success")

			// Collect metrics
			result := telemetry.CollectMetrics()
			require.NotNil(t, result)

			// Find the TestScan metric
			var scanMetric *telemetry.Summary
			for i := range result.Operations {
				if result.Operations[i].Operation == "TestScan" {
					scanMetric = &result.Operations[i]

					break
				}
			}

			if scanMetric != nil {
				// 0.0001234 seconds = 0.1234 milliseconds, so we should see a very small value
				assert.Greater(t, scanMetric.AvgMs, 0.0)
				assert.Less(t, scanMetric.AvgMs, 1.0, "expected sub-millisecond operation")
			}
		}},
		{name: "CollectMetrics_RuntimeMetrics", run: func(t *testing.T) {
			// Collect metrics
			result := telemetry.CollectMetrics()
			require.NotNil(t, result)

			// Verify runtime metrics are present
			assert.Positive(t, result.Runtime.Goroutines, "expected at least 1 goroutine")
			assert.GreaterOrEqual(t, result.Runtime.HeapAllocMB, 0.0, "heap allocation should be >= 0")
			assert.GreaterOrEqual(t, result.Runtime.HeapSysMB, 0.0, "heap sys should be >= 0")
			// NumGC is uint32, always >= 0, no need to assert
			assert.GreaterOrEqual(t, result.Runtime.LastGCPause, 0.0, "last GC pause should be >= 0")
			assert.GreaterOrEqual(t, result.Runtime.TotalAllocMB, 0.0, "total alloc should be >= 0")

			// Heap alloc should be less than or equal to heap sys
			assert.LessOrEqual(t, result.Runtime.HeapAllocMB, result.Runtime.HeapSysMB,
				"heap alloc should not exceed heap sys")
		}},
		{name: "RecordDeleteQueueDepth", run: func(t *testing.T) {
			// Smoke test — should not panic.
			telemetry.RecordDeleteQueueDepth("s3", 5)
			telemetry.RecordDeleteQueueDepth("dynamodb", 0)
		}},
		{name: "RecordTTLEvictions", run: func(t *testing.T) {
			telemetry.RecordTTLEvictions("dynamodb", 10)
			telemetry.RecordTTLEvictions("dynamodb", 0)
		}},
		{name: "RecordStreamEvents", run: func(t *testing.T) {
			telemetry.RecordStreamEvents("dynamodb", 3)
			// count == 0 should be a no-op (no panic)
			telemetry.RecordStreamEvents("dynamodb", 0)
		}},
		{name: "RecordWorkerTask", run: func(t *testing.T) {
			telemetry.RecordWorkerTask("dynamodb", "TableCleaner", "success")
			telemetry.RecordWorkerTask("s3", "BucketJanitor", "error")
		}},
		{name: "RecordWorkerItems", run: func(t *testing.T) {
			telemetry.RecordWorkerItems("dynamodb", "TTLSweeper", 42)
			// count == 0 should be a no-op (no panic)
			telemetry.RecordWorkerItems("dynamodb", "TTLSweeper", 0)
		}},
		{name: "RecordWorkerQueueDepth", run: func(t *testing.T) {
			telemetry.RecordWorkerQueueDepth("s3", "BucketJanitor", 7)
			telemetry.RecordWorkerQueueDepth("s3", "BucketJanitor", 0)
		}},
		{name: "GetMetrics", run: func(t *testing.T) {
			result := telemetry.GetMetrics()
			require.NotNil(t, result)
			_, ok := result["operations"]
			assert.True(t, ok, "expected 'operations' key in GetMetrics result")
		}},
		{name: "WrapEchoHandler_Success", run: func(t *testing.T) {
			log := slog.Default()
			obs := &mockObserver{operation: "GetItem", resource: "MyTable"}
			handler := telemetry.WrapEchoHandler("dynamodb", func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			}, obs, log)

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := handler(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, rec.Code)
		}},
		{name: "WrapEchoHandler_HandlerError", run: func(t *testing.T) {
			log := slog.Default()
			obs := &mockObserver{operation: "PutItem", resource: "MyTable"}
			handler := telemetry.WrapEchoHandler("dynamodb", func(_ *echo.Context) error {
				return errBoom
			}, obs, log)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := handler(c)
			assert.ErrorIs(t, err, errBoom)
		}},
		{name: "WrapEchoHandler_UnknownOperation", run: func(t *testing.T) {
			log := slog.Default()
			// When operation starts as "Unknown", the wrapper re-extracts after the handler runs.
			callCount := 0
			obs := &refiningObserver{
				callCount:  &callCount,
				first:      "Unknown",
				subsequent: "Resolved",
				resource:   "table",
			}

			handler := telemetry.WrapEchoHandler("s3", func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			}, obs, log)

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := handler(c)
			require.NoError(t, err)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
