package cloudwatch_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

func TestHandler_Dashboard_PutGetListDelete(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(t, h, url.Values{
		"Action":        []string{"PutDashboard"},
		"DashboardName": []string{"myboard"},
		"DashboardBody": []string{`{"widgets":[]}`},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, url.Values{
		"Action":        []string{"GetDashboard"},
		"DashboardName": []string{"myboard"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "myboard")

	rec = postForm(t, h, "Action=ListDashboards")
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "myboard")

	rec = postForm(t, h, url.Values{
		"Action":                  []string{"DeleteDashboards"},
		"DashboardNames.member.1": []string{"myboard"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, url.Values{
		"Action":        []string{"GetDashboard"},
		"DashboardName": []string{"myboard"},
	}.Encode())
	assert.Equal(t, 400, rec.Code)
}

func TestHandler_Dashboard_NamePrefix_Filter(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	for _, name := range []string{"prod-dashboard", "staging-board", "prod-metrics"} {
		postForm(t, h, url.Values{
			"Action":        []string{"PutDashboard"},
			"DashboardName": []string{name},
			"DashboardBody": []string{"{}"},
		}.Encode())
	}

	rec := postForm(t, h, "Action=ListDashboards&DashboardNamePrefix=prod-")
	assert.Equal(t, 200, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "prod-dashboard")
	assert.Contains(t, body, "prod-metrics")
	assert.NotContains(t, body, "staging-board")
}

func TestCloudWatchHandler_Dashboards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T, h *cloudwatch.Handler)
		name            string
		body            string
		wantContains    []string
		wantNotContains []string
		wantCode        int
	}{
		{
			name:         "PutDashboard/success",
			body:         `Action=PutDashboard&DashboardName=MyDash&DashboardBody={"widgets":[]}`,
			wantCode:     http.StatusOK,
			wantContains: []string{"PutDashboardResponse"},
		},
		{
			name:     "PutDashboard/missing name",
			body:     `Action=PutDashboard&DashboardBody={}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "GetDashboard/success",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, `Action=PutDashboard&DashboardName=FetchMe&DashboardBody={"widgets":[]}`)
			},
			body:         "Action=GetDashboard&DashboardName=FetchMe",
			wantCode:     http.StatusOK,
			wantContains: []string{"GetDashboardResponse", "FetchMe"},
		},
		{
			name:     "GetDashboard/not found",
			body:     "Action=GetDashboard&DashboardName=Ghost",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "GetDashboard/missing name",
			body:     "Action=GetDashboard",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "ListDashboards/success",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, `Action=PutDashboard&DashboardName=prod-web&DashboardBody={}`)
				postForm(t, h, `Action=PutDashboard&DashboardName=prod-api&DashboardBody={}`)
			},
			body:         "Action=ListDashboards",
			wantCode:     http.StatusOK,
			wantContains: []string{"ListDashboardsResponse", "prod-web", "prod-api"},
		},
		{
			name: "ListDashboards/with prefix",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, `Action=PutDashboard&DashboardName=prod-web&DashboardBody={}`)
				postForm(t, h, `Action=PutDashboard&DashboardName=staging-web&DashboardBody={}`)
			},
			body:            "Action=ListDashboards&DashboardNamePrefix=prod-",
			wantCode:        http.StatusOK,
			wantContains:    []string{"prod-web"},
			wantNotContains: []string{"staging-web"},
		},
		{
			name:         "ListDashboards/empty",
			body:         "Action=ListDashboards",
			wantCode:     http.StatusOK,
			wantContains: []string{"ListDashboardsResponse"},
		},
		{
			// "DeleteDashboardsResponse" alone can't fail: it's the substring
			// gopherstack-jodk's bug (missing <DeleteDashboardsResult>) also
			// produced. TestCloudWatchQueryProtocol_ResultWrapperPresent in
			// query_result_wrapper_test.go asserts the real query/XML
			// deserializer finds the Result element; this check additionally
			// pins the wrapper's literal presence in the body.
			name: "DeleteDashboards/success",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, `Action=PutDashboard&DashboardName=to-delete&DashboardBody={}`)
			},
			body:         "Action=DeleteDashboards&DashboardNames.member.1=to-delete",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteDashboardsResponse", "<DeleteDashboardsResult"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newCWHandler()
			if tt.setup != nil {
				tt.setup(t, h)
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

func TestCloudWatchHandler_GetSupportedOperations_DashboardOps(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "PutDashboard")
	assert.Contains(t, ops, "GetDashboard")
	assert.Contains(t, ops, "ListDashboards")
	assert.Contains(t, ops, "DeleteDashboards")
}
