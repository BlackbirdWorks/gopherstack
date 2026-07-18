package cloudwatch_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler_MetricFilter_FullCycle(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	rec := postForm(t, h, url.Values{
		"Action":        []string{"PutMetricFilter"},
		"FilterName":    []string{"f1"},
		"LogGroupName":  []string{"/app/logs"},
		"FilterPattern": []string{"[ERROR]"},
		"MetricTransformations.member.1.MetricName":      []string{"ErrCount"},
		"MetricTransformations.member.1.MetricNamespace": []string{"App"},
		"MetricTransformations.member.1.MetricValue":     []string{"1"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, "Action=DescribeMetricFilters&LogGroupName=%2Fapp%2Flogs")
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "f1")

	rec = postForm(t, h, "Action=DeleteMetricFilter&FilterName=f1&LogGroupName=%2Fapp%2Flogs")
	assert.Equal(t, 200, rec.Code)
}

func TestCloudWatchHandler_PutMetricFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		body           string
		wantStatusCode int
	}{
		{
			name: "valid",
			body: "Action=PutMetricFilter&FilterName=my-filter&LogGroupName=%2Faws%2Flambda%2Ffn&FilterPattern=%5B*%5D" +
				"&MetricTransformations.member.1.MetricName=Count" +
				"&MetricTransformations.member.1.MetricNamespace=MyApp" +
				"&MetricTransformations.member.1.MetricValue=1",
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "missing_filter_name",
			body:           "Action=PutMetricFilter&LogGroupName=%2Faws%2Flambda%2Ffn",
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name:           "missing_log_group",
			body:           "Action=PutMetricFilter&FilterName=my-filter",
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

func TestCloudWatchHandler_DescribeMetricFilters(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	postForm(t, h, "Action=PutMetricFilter&FilterName=alpha&LogGroupName=%2Faws%2Flambda%2Ffn1&FilterPattern=a")
	postForm(t, h, "Action=PutMetricFilter&FilterName=beta&LogGroupName=%2Faws%2Flambda%2Ffn1&FilterPattern=b")
	postForm(t, h, "Action=PutMetricFilter&FilterName=gamma&LogGroupName=%2Faws%2Fec2&FilterPattern=c")

	rec := postForm(t, h, "Action=DescribeMetricFilters&LogGroupName=%2Faws%2Flambda%2Ffn1")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "alpha")
	assert.Contains(t, rec.Body.String(), "beta")
	assert.NotContains(t, rec.Body.String(), "gamma")
}

func TestCloudWatchHandler_DeleteMetricFilter(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	postForm(t, h, "Action=PutMetricFilter&FilterName=del-filter&LogGroupName=%2Faws%2Flambda%2Ffn&FilterPattern=x")

	rec := postForm(t, h, "Action=DeleteMetricFilter&FilterName=del-filter&LogGroupName=%2Faws%2Flambda%2Ffn")
	assert.Equal(t, http.StatusOK, rec.Code)

	// second delete should 400
	rec2 := postForm(t, h, "Action=DeleteMetricFilter&FilterName=del-filter&LogGroupName=%2Faws%2Flambda%2Ffn")
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}
