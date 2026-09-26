package cloudwatch_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// seedPutMetricDataBenchBackend seeds one metric with exactly
// CwMaxMetricDataPointsForTest points already stored, so the benchmark
// measures storeDatum's steady-state (at-cap) cost -- the case every
// long-running metric hits once it accumulates its first 1000 points.
func seedPutMetricDataBenchBackend(b *testing.B) *cloudwatch.Handler {
	b.Helper()

	bk := cloudwatch.NewInMemoryBackend()
	h := cloudwatch.NewHandler(bk)

	base := time.Now().UTC().Add(-2 * time.Hour)
	for i := range cloudwatch.CwMaxMetricDataPointsForTest {
		v := float64(i)
		err := bk.PutMetricData("Bench/Namespace", []cloudwatch.MetricDatum{{
			MetricName: "Requests",
			Timestamp:  base.Add(time.Duration(i) * time.Second),
			Value:      v,
			HasValue:   true,
			Count:      1,
			Sum:        v,
			Min:        v,
			Max:        v,
		}})
		require.NoError(b, err)
	}

	return h
}

// BenchmarkPutMetricData_AtCap measures the steady-state cost once a metric
// already holds cwMaxMetricDataPoints entries -- every subsequent datum used
// to force two allocations (append growth + a full cap-sized copy).
func BenchmarkPutMetricData_AtCap(b *testing.B) {
	h := seedPutMetricDataBenchBackend(b)
	e := echo.New()
	ts := time.Now().UTC().Format(time.RFC3339)

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		body := "Action=PutMetricData" +
			"&Namespace=Bench/Namespace" +
			"&MetricData.member.1.MetricName=Requests" +
			"&MetricData.member.1.Value=" + strconv.Itoa(i) +
			"&MetricData.member.1.Timestamp=" + ts

		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		if err := h.Handler()(c); err != nil {
			b.Fatal(err)
		}

		if rec.Code != http.StatusOK {
			b.Fatalf("status = %d", rec.Code)
		}
	}
}
