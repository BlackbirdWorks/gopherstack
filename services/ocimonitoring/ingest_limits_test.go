package ocimonitoring_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/monitoring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	oci "github.com/blackbirdworks/gopherstack/services/ocimonitoring"
)

func TestStreamPointCap(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	base := e.clock.Now().Add(-time.Hour)

	const total = 100000 + 7

	for from := 0; from < total; from += 20000 {
		dps := make([]oci.DatapointIn, 0, 20000)

		for i := from; i < min(from+20000, total); i++ {
			ts, v := base.Add(time.Duration(i)*10*time.Millisecond), 1.0
			dps = append(dps, oci.DatapointIn{Timestamp: &ts, Value: &v})
		}

		out, err := e.backend.PostMetricData(oci.PostMetricDataIn{MetricData: []oci.MetricDataIn{{
			Namespace: ns, CompartmentID: testCompartment, Name: metric, Dimensions: map[string]string{"h": "a"},
			Datapoints: dps,
		}}})
		require.NoError(t, err)
		require.Empty(t, out.FailedMetrics)
	}

	start, end := base.Add(-time.Minute), e.clock.Now()

	series, err := e.backend.SummarizeMetricsData(testCompartment, oci.SummarizeIn{
		StartTime: &start, EndTime: &end, Namespace: ns, Query: metric + "[1h].count()", Resolution: "1h",
	})
	require.NoError(t, err)
	require.Len(t, series, 1)

	var sum float64
	for _, p := range series[0].AggregatedDatapoints {
		sum += p.Value
	}

	assert.InDelta(t, 100000, sum, 0, "the oldest points past the per-stream cap are dropped")
}

func gzipped(t *testing.T, s string) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	_, err := zw.Write([]byte(s))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func TestContentEncoding(t *testing.T) {
	t.Parallel()

	ts := newFakeClock().Now().Add(-time.Minute).Format(time.RFC3339Nano)
	payload := `{"metricData":[{"namespace":"enc_ns","compartmentId":"` + testCompartment +
		`","name":"M","dimensions":{"h":"a"},"datapoints":[{"timestamp":"` + ts + `","value":3}]}]}`

	tests := []struct {
		name     string
		encoding string
		body     []byte
		status   int
		stored   bool
	}{
		{"gzip", "gzip", gzipped(t, payload), http.StatusOK, true},
		{"gzip upper", "GZIP", gzipped(t, payload), http.StatusOK, true},
		{"identity", "identity", []byte(payload), http.StatusOK, true},
		{"bad gzip", "gzip", []byte(payload), http.StatusBadRequest, false},
		{"unsupported", "br", []byte(payload), http.StatusBadRequest, false},
		{"bomb", "gzip", gzipped(t, strings.Repeat(" ", 2<<20)+payload), http.StatusRequestEntityTooLarge, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newEnv(t, envOpts{})

			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, env.base+"/20180401/metrics",
				bytes.NewReader(tt.body))
			require.NoError(t, err)

			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Content-Encoding", tt.encoding)

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			assert.Equal(t, tt.status, resp.StatusCode)

			list, err := env.mon.ListMetrics(t.Context(), monitoring.ListMetricsRequest{
				CompartmentId: new(testCompartment), Namespace: new("enc_ns"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.stored, len(list.Items) == 1)
		})
	}
}
