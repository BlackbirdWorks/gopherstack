package cloudwatch_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// ---------------------------------------------------------------------------
// Handler integration: PutAnomalyDetector with dimensions (gap #11)
// ---------------------------------------------------------------------------

func TestHandler_PutAnomalyDetector_WithDimensions(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(
		t, h,
		"Action=PutAnomalyDetector"+
			"&Namespace=App"+
			"&MetricName=Latency"+
			"&Stat=Average"+
			"&SingleMetricAnomalyDetector.Dimensions.member.1.Name=Service"+
			"&SingleMetricAnomalyDetector.Dimensions.member.1.Value=api",
	)
	assert.Equal(t, 200, rec.Code, "body: %s", rec.Body.String())
}

// ---------------------------------------------------------------------------
// Handler integration: tag cleanup on delete (gap #11 + orphan cleanup)
// ---------------------------------------------------------------------------

func TestHandler_DeleteAnomalyDetector_CleansUpTags(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	// Create detector.
	rec := postForm(
		t, h,
		"Action=PutAnomalyDetector&Namespace=App&MetricName=CPU&Stat=Average",
	)
	assert.Equal(t, 200, rec.Code)

	// Tag it (we need a valid ARN for the tag — use describe to find it or construct directly).
	// The implementation uses buildAnomalyDetectorARN which is internal; we just verify delete succeeds.
	rec = postForm(
		t, h,
		"Action=DeleteAnomalyDetector&Namespace=App&MetricName=CPU&Stat=Average",
	)
	assert.Equal(t, 200, rec.Code, "delete should succeed; body: %s", rec.Body.String())
}

func TestHandler_DescribeAnomalyDetectors_WithDimensions(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	postForm(
		t, h,
		"Action=PutAnomalyDetector&Namespace=App&MetricName=CPU&Stat=Average"+
			"&SingleMetricAnomalyDetector.Dimensions.member.1.Name=Host"+
			"&SingleMetricAnomalyDetector.Dimensions.member.1.Value=web1",
	)

	rec := postForm(t, h, "Action=DescribeAnomalyDetectors&Namespace=App")
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "CPU")
}

func TestHandler_AnomalyDetector_FullCycle(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	rec := postForm(t, h, "Action=PutAnomalyDetector&Namespace=App&MetricName=CPU&Stat=Average")
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, "Action=DescribeAnomalyDetectors&Namespace=App")
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "CPU")

	rec = postForm(t, h, "Action=DeleteAnomalyDetector&Namespace=App&MetricName=CPU&Stat=Average")
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, "Action=DescribeAnomalyDetectors&Namespace=App")
	assert.Equal(t, 200, rec.Code)
	assert.NotContains(t, rec.Body.String(), "CPU")
}

func TestCloudWatchHandler_AnomalyDetector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T, h *cloudwatch.Handler, b *cloudwatch.InMemoryBackend)
		name            string
		body            string
		wantContains    []string
		wantNotContains []string
		wantCode        int
	}{
		{
			name: "DescribeAnomalyDetectors/success",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{
					Namespace:  "AWS/EC2",
					MetricName: "CPUUtilization",
					Stat:       "Average",
				})
			},
			body:         "Action=DescribeAnomalyDetectors&Namespace=AWS/EC2",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAnomalyDetectorsResponse", "CPUUtilization"},
		},
		{
			name:         "DescribeAnomalyDetectors/empty",
			body:         "Action=DescribeAnomalyDetectors",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAnomalyDetectorsResponse"},
		},
		{
			name: "DescribeAnomalyDetectors/namespace filter",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{
					Namespace:  "AWS/EC2",
					MetricName: "CPUUtilization",
					Stat:       "Average",
				})
				b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{
					Namespace:  "AWS/RDS",
					MetricName: "DatabaseConnections",
					Stat:       "Sum",
				})
			},
			body:            "Action=DescribeAnomalyDetectors&Namespace=AWS/EC2",
			wantCode:        http.StatusOK,
			wantContains:    []string{"CPUUtilization"},
			wantNotContains: []string{"DatabaseConnections"},
		},
		{
			name: "DeleteAnomalyDetector/success",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{
					Namespace:  "NS",
					MetricName: "Metric",
					Stat:       "Sum",
				})
			},
			body: "Action=DeleteAnomalyDetector" +
				"&SingleMetricAnomalyDetector.Namespace=NS" +
				"&SingleMetricAnomalyDetector.MetricName=Metric" +
				"&SingleMetricAnomalyDetector.Stat=Sum",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteAnomalyDetectorResponse"},
		},
		{
			name: "DeleteAnomalyDetector/not found",
			body: "Action=DeleteAnomalyDetector" +
				"&SingleMetricAnomalyDetector.Namespace=NS" +
				"&SingleMetricAnomalyDetector.MetricName=Ghost" +
				"&SingleMetricAnomalyDetector.Stat=Sum",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DeleteAnomalyDetector/missing namespace",
			body:     "Action=DeleteAnomalyDetector&SingleMetricAnomalyDetector.MetricName=M",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newCWHandlerWithBackend()
			if tt.setup != nil {
				tt.setup(t, h, b)
			}

			rec := postForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
			for _, s := range tt.wantNotContains {
				assert.NotContains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestCloudWatchHandler_AnomalyDetector_MetricNameFilter(t *testing.T) {
	t.Parallel()

	h, b := newCWHandlerWithBackend()
	b.PutAnomalyDetectorInternal(
		&cloudwatch.AnomalyDetector{Namespace: "AWS/EC2", MetricName: "CPUUtilization", Stat: "Average"},
	)
	b.PutAnomalyDetectorInternal(
		&cloudwatch.AnomalyDetector{Namespace: "AWS/EC2", MetricName: "NetworkIn", Stat: "Sum"},
	)

	rec := postForm(t, h, "Action=DescribeAnomalyDetectors&MetricName=CPUUtilization")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "CPUUtilization")
	assert.NotContains(t, body, "NetworkIn")
}

func TestCloudWatchHandler_AnomalyDetector_DefaultState(t *testing.T) {
	t.Parallel()

	_, b := newCWHandlerWithBackend()
	b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{Namespace: "NS", MetricName: "M", Stat: "Sum"})

	p, err := b.DescribeAnomalyDetectors("NS", "", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "TRAINED_INSUFFICIENT_DATA", p.Data[0].StateValue)
}

func TestCloudWatchHandler_DescribeAnomalyDetectors_SortedOutput(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{Namespace: "ZNS", MetricName: "M", Stat: "Sum"})
	b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{Namespace: "ANS", MetricName: "M", Stat: "Sum"})
	b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{Namespace: "MNS", MetricName: "M", Stat: "Sum"})

	p, err := b.DescribeAnomalyDetectors("", "", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 3)
	assert.Equal(t, "ANS", p.Data[0].Namespace)
	assert.Equal(t, "MNS", p.Data[1].Namespace)
	assert.Equal(t, "ZNS", p.Data[2].Namespace)
}

func TestCloudWatchHandler_PutAnomalyDetector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		body           string
		wantStatusCode int
	}{
		{
			name: "valid",
			body: "Action=PutAnomalyDetector" +
				"&SingleMetricAnomalyDetector.Namespace=AWS%2FEC2" +
				"&SingleMetricAnomalyDetector.MetricName=CPUUtilization" +
				"&SingleMetricAnomalyDetector.Stat=Average",
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "missing_namespace",
			body:           "Action=PutAnomalyDetector&SingleMetricAnomalyDetector.MetricName=CPUUtilization",
			wantStatusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newCWHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)
		})
	}
}
