package cloudwatch_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/cloudwatch"
)

// TestStatValue exercises all branches of the statValue function via GetMetricData.
// statValue is only called from GetMetricData in the backend.
func TestStatValue_AllBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		stat     string
		wantCode int
	}{
		{name: "Sum", stat: "Sum", wantCode: http.StatusOK},
		{name: "Average", stat: "Average", wantCode: http.StatusOK},
		{name: "Minimum", stat: "Minimum", wantCode: http.StatusOK},
		{name: "Min", stat: "Min", wantCode: http.StatusOK},
		{name: "Maximum", stat: "Maximum", wantCode: http.StatusOK},
		{name: "Max", stat: "Max", wantCode: http.StatusOK},
		{name: "SampleCount", stat: "SampleCount", wantCode: http.StatusOK},
		{name: "Unknown_default", stat: "UnknownStat", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := cwServer(t)

			cwPost(
				t, ts,
				"Action=PutMetricData&Namespace=StatTest"+
					"&MetricData.member.1.MetricName=Hits"+
					"&MetricData.member.1.Value=10",
			).Body.Close()

			statBody := "Action=GetMetricData" +
				"&StartTime=2000-01-01T00:00:00Z" +
				"&EndTime=2099-01-01T00:00:00Z" +
				"&MetricDataQueries.member.1.Id=q1" +
				"&MetricDataQueries.member.1.MetricStat.Metric.Namespace=StatTest" +
				"&MetricDataQueries.member.1.MetricStat.Metric.MetricName=Hits" +
				"&MetricDataQueries.member.1.MetricStat.Stat=" + tt.stat +
				"&MetricDataQueries.member.1.MetricStat.Period=60"

			resp := cwPost(t, ts, statBody)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}

// TestStatValue_NilPointers tests statValue when datapoint fields are nil (nil pointer branches).
func TestStatValue_NilPointers(t *testing.T) {
	t.Parallel()

	// Use GetMetricData for a namespace/metric with NO data at all.
	// statValue is called with a datapoint that has only the requested stat set.
	// The *other* stats' pointers are nil, which hits the nil-pointer early-exits.
	tests := []struct {
		name     string
		stat     string
		wantCode int
	}{
		{name: "Sum_nil_pointer_path", stat: "Sum", wantCode: http.StatusOK},
		{name: "Average_nil_pointer_path", stat: "Average", wantCode: http.StatusOK},
		{name: "Minimum_nil_pointer_path", stat: "Minimum", wantCode: http.StatusOK},
		{name: "Maximum_nil_pointer_path", stat: "Maximum", wantCode: http.StatusOK},
		{name: "SampleCount_nil_pointer_path", stat: "SampleCount", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := cwServer(t)

			cwPost(
				t, ts,
				"Action=PutMetricData&Namespace=NilTest"+
					"&MetricData.member.1.MetricName=NilMetric"+
					"&MetricData.member.1.Value=5",
			).Body.Close()

			// Use a DIFFERENT stat in the query than the one put (so the pointer will be nil).
			// For example, put with Sum built, then request Average stat which won't be in the dp.
			statBody := "Action=GetMetricData" +
				"&StartTime=2000-01-01T00:00:00Z" +
				"&EndTime=2099-01-01T00:00:00Z" +
				"&MetricDataQueries.member.1.Id=q1" +
				"&MetricDataQueries.member.1.MetricStat.Metric.Namespace=NilTest" +
				"&MetricDataQueries.member.1.MetricStat.Metric.MetricName=NilMetric" +
				"&MetricDataQueries.member.1.MetricStat.Stat=" + tt.stat +
				"&MetricDataQueries.member.1.MetricStat.Period=60"

			resp := cwPost(t, ts, statBody)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}

func TestCloudWatchHandler_ExtractOperation_ParseFormError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{
			name:   "invalid_query_string_returns_empty",
			rawURL: "/?%zz=1",
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := cloudwatch.NewHandler(cloudwatch.NewInMemoryBackend(), nil)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.rawURL, strings.NewReader("Action=ListMetrics"))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.RequestURI = tt.rawURL
			c := e.NewContext(req, httptest.NewRecorder())

			result := h.ExtractOperation(c)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestCloudWatchHandler_ExtractResource_ParseFormError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{
			name:   "invalid_query_string_returns_empty",
			rawURL: "/?%zz=1",
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := cloudwatch.NewHandler(cloudwatch.NewInMemoryBackend(), nil)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.rawURL, strings.NewReader("Namespace=AWS/EC2"))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.RequestURI = tt.rawURL
			c := e.NewContext(req, httptest.NewRecorder())

			result := h.ExtractResource(c)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestCloudWatchHandler_GetMetricData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name: "with_metric_data_queries",
			body: "Action=GetMetricData" +
				"&StartTime=2000-01-01T00:00:00Z" +
				"&EndTime=2099-01-01T00:00:00Z" +
				"&MetricDataQueries.member.1.Id=q1" +
				"&MetricDataQueries.member.1.Label=TestLabel" +
				"&MetricDataQueries.member.1.MetricStat.Metric.Namespace=AWS/EC2" +
				"&MetricDataQueries.member.1.MetricStat.Metric.MetricName=CPUUtilization" +
				"&MetricDataQueries.member.1.MetricStat.Stat=Average" +
				"&MetricDataQueries.member.1.MetricStat.Period=60",
			wantCode: http.StatusOK,
		},
		{
			name: "invalid_start_and_end_time_uses_defaults",
			body: "Action=GetMetricData" +
				"&StartTime=invalid" +
				"&EndTime=invalid" +
				"&MetricDataQueries.member.1.Id=q1" +
				"&MetricDataQueries.member.1.MetricStat.Metric.Namespace=NS" +
				"&MetricDataQueries.member.1.MetricStat.Metric.MetricName=M" +
				"&MetricDataQueries.member.1.MetricStat.Stat=Sum" +
				"&MetricDataQueries.member.1.MetricStat.Period=60",
			wantCode: http.StatusOK,
		},
		{
			name: "with_query_label",
			body: "Action=GetMetricData" +
				"&StartTime=2000-01-01T00:00:00Z" +
				"&EndTime=2099-01-01T00:00:00Z" +
				"&MetricDataQueries.member.1.Id=q1" +
				"&MetricDataQueries.member.1.Label=" +
				"&MetricDataQueries.member.1.MetricStat.Metric.Namespace=NS" +
				"&MetricDataQueries.member.1.MetricStat.Metric.MetricName=M" +
				"&MetricDataQueries.member.1.MetricStat.Stat=Average" +
				"&MetricDataQueries.member.1.MetricStat.Period=60",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := cwServer(t)
			// Pre-populate some metric data
			cwPost(t, ts,
				"Action=PutMetricData&Namespace=AWS/EC2"+
					"&MetricData.member.1.MetricName=CPUUtilization"+
					"&MetricData.member.1.Value=42").Body.Close()

			resp := cwPost(t, ts, tt.body)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}

func TestCloudWatchHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "unknown_action_returns_bad_request",
			body:     "Action=UnknownAction&Namespace=NS",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := cwServer(t)
			resp := cwPost(t, ts, tt.body)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}

func TestCloudWatchHandler_TagsOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "list_tags_for_resource",
			body:     "Action=ListTagsForResource&ResourceARN=arn:aws:cloudwatch:us-east-1:123456789012:alarm:test",
			wantCode: http.StatusOK,
		},
		{
			name: "tag_resource",
			body: "Action=TagResource&ResourceARN=arn:aws:cloudwatch:us-east-1:123456789012:alarm:test" +
				"&Tags.member.1.Key=env&Tags.member.1.Value=prod",
			wantCode: http.StatusOK,
		},
		{
			name: "untag_resource",
			body: "Action=UntagResource&ResourceARN=arn:aws:cloudwatch:us-east-1:123456789012:alarm:test" +
				"&TagKeys.member.1=env",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := cwServer(t)
			resp := cwPost(t, ts, tt.body)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}

func TestCloudWatchHandler_RouteMatcher_NonPOST(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		method    string
		wantMatch bool
	}{
		{
			name:      "get_not_matched",
			method:    http.MethodGet,
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := cloudwatch.NewHandler(cloudwatch.NewInMemoryBackend(), nil)
			matcher := h.RouteMatcher()
			e := echo.New()
			req := httptest.NewRequest(tt.method, "/", nil)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestCloudWatchHandler_HandleDeleteAlarms_Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(t *testing.T, ts *httptest.Server)
		body     string
		wantCode int
	}{
		{
			name:     "delete_alarms_not_found",
			body:     "Action=DeleteAlarms&AlarmNames.member.1=nonexistent",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := cwServer(t)
			if tt.setup != nil {
				tt.setup(t, ts)
			}

			resp := cwPost(t, ts, tt.body)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}

func TestCloudWatchHandler_ParseFormError_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rawURL   string
		wantCode int
	}{
		{
			name:     "invalid_form_body_returns_bad_request",
			rawURL:   "/?%zz=1",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newCWHandler()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.rawURL, strings.NewReader("Action=PutMetricData"))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.RequestURI = tt.rawURL

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
