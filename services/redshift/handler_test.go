package redshift_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

func newRedshiftHandler() *redshift.Handler {
	return redshift.NewHandler(redshift.NewInMemoryBackend("000000000000", "us-east-1"))
}

func newRedshiftBackend() *redshift.InMemoryBackend {
	return redshift.NewInMemoryBackend("000000000000", "us-east-1")
}

func postRedshiftForm(t *testing.T, h *redshift.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestRedshiftHandler_CreateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=test-cluster&" +
				"NodeType=dc2.large&DBName=mydb&MasterUsername=admin",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateClusterResponse", "test-cluster"},
		},
		{
			name:         "empty_id",
			body:         "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestRedshiftHandler_DeleteCluster(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=del-cluster")

	rec := postRedshiftForm(t, h, "Action=DeleteCluster&Version=2012-12-01&ClusterIdentifier=del-cluster")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteClusterResponse")
}

func TestRedshiftHandler_DescribeClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "list_all",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=desc-cluster")
			},
			body:         "Action=DescribeClusters&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeClustersResponse", "desc-cluster"},
		},
		{
			name:     "not_found",
			body:     "Action=DescribeClusters&Version=2012-12-01&ClusterIdentifier=nonexistent",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestRedshiftHandler_InvalidAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "invalid_action",
			body:     "Action=InvalidAction&Version=2012-12-01",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_action",
			body:     "Version=2012-12-01",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestRedshiftHandler_DeleteCluster_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=del-cluster")
			},
			body:         "Action=DeleteCluster&Version=2012-12-01&ClusterIdentifier=del-cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteClusterResponse", "del-cluster"},
		},
		{
			name:     "not_found",
			body:     "Action=DeleteCluster&Version=2012-12-01&ClusterIdentifier=nonexistent",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_id",
			body:     "Action=DeleteCluster&Version=2012-12-01&ClusterIdentifier=",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestRedshiftHandler_DescribeTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeTagsResponse"},
		},
		{
			name: "with_tags",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=tag-cluster")
				postRedshiftForm(t, h, "Action=CreateTags&Version=2012-12-01&ResourceName=tag-cluster&"+
					"Tags.Tag.1.Key=env&Tags.Tag.1.Value=prod")
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeTagsResponse", "env", "prod", "tag-cluster"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := postRedshiftForm(t, h, "Action=DescribeTags&Version=2012-12-01")
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestRedshiftHandler_CreateTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=ct-cluster")
			},
			body: "Action=CreateTags&Version=2012-12-01&ResourceName=ct-cluster&" +
				"Tags.Tag.1.Key=env&Tags.Tag.1.Value=prod&" +
				"Tags.Tag.2.Key=team&Tags.Tag.2.Value=platform",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateTagsResponse"},
		},
		{
			name: "cluster_not_found",
			body: "Action=CreateTags&Version=2012-12-01&ResourceName=nonexistent&" +
				"Tags.Tag.1.Key=env&Tags.Tag.1.Value=prod",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestRedshiftHandler_DeleteTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=dt-cluster")
				postRedshiftForm(t, h, "Action=CreateTags&Version=2012-12-01&ResourceName=dt-cluster&"+
					"Tags.Tag.1.Key=env&Tags.Tag.1.Value=prod&Tags.Tag.2.Key=team&Tags.Tag.2.Value=platform")
			},
			body: "Action=DeleteTags&Version=2012-12-01&ResourceName=dt-cluster&" +
				"TagKeys.TagKey.1=env",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteTagsResponse"},
		},
		{
			name:     "cluster_not_found",
			body:     "Action=DeleteTags&Version=2012-12-01&ResourceName=nonexistent&TagKeys.TagKey.1=env",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestRedshiftHandler_DescribeLoggingStatus(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	rec := postRedshiftForm(t, h, "Action=DescribeLoggingStatus&Version=2012-12-01")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeLoggingStatusResponse")
	assert.Contains(t, rec.Body.String(), "LoggingEnabled")
}

// mockDNSRegistrar is a test double for redshift.DNSRegistrar.
type mockDNSRegistrar struct {
	registered map[string]bool
}

func (m *mockDNSRegistrar) Register(hostname string) {
	m.registered[hostname] = true
}

func (m *mockDNSRegistrar) Deregister(hostname string) {
	delete(m.registered, hostname)
}

func TestRedshiftBackend_DNSRegistrar(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		clusterID      string
		wantRegistered bool
		deleteAfter    bool
	}{
		{
			name:           "registers_on_create",
			clusterID:      "my-cluster",
			wantRegistered: true,
		},
		{
			name:           "deregisters_on_delete",
			clusterID:      "del-cluster",
			deleteAfter:    true,
			wantRegistered: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			registrar := &mockDNSRegistrar{registered: make(map[string]bool)}
			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			b.SetDNSRegistrar(registrar)

			cluster, err := b.CreateCluster(tt.clusterID, "dc2.large", "dev", "admin")
			require.NoError(t, err)

			if tt.deleteAfter {
				_, err = b.DeleteCluster(tt.clusterID)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantRegistered, registrar.registered[cluster.Endpoint])
		})
	}
}

// ----- DeleteCluster SkipFinalClusterSnapshot -----

// TestDeleteCluster_FinalSnapshot verifies that DeleteCluster respects
// SkipFinalClusterSnapshot and FinalClusterSnapshotIdentifier. Real AWS:
//   - Requires FinalClusterSnapshotIdentifier when SkipFinalClusterSnapshot=false
//   - Creates a snapshot before deletion when FinalClusterSnapshotIdentifier is provided
func TestDeleteCluster_FinalSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		skipFinal         string
		finalSnapshotID   string
		wantErr           string
		wantCode          int
		wantSnapshotAfter bool
	}{
		{
			name:              "skip_true_no_snapshot_id_succeeds",
			skipFinal:         "true",
			finalSnapshotID:   "",
			wantCode:          http.StatusOK,
			wantSnapshotAfter: false,
		},
		{
			name:              "skip_false_with_snapshot_id_creates_snapshot",
			skipFinal:         "false",
			finalSnapshotID:   "final-snap-1",
			wantCode:          http.StatusOK,
			wantSnapshotAfter: true,
		},
		{
			name:            "skip_false_without_snapshot_id_returns_error",
			skipFinal:       "false",
			finalSnapshotID: "",
			wantCode:        http.StatusBadRequest,
			wantErr:         "FinalClusterSnapshotIdentifier",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			clusterID := "del-" + strings.ReplaceAll(tt.name, "_", "-")

			postRedshiftForm(t, h,
				"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier="+clusterID)

			body := "Action=DeleteCluster&Version=2012-12-01&ClusterIdentifier=" + clusterID +
				"&SkipFinalClusterSnapshot=" + tt.skipFinal
			if tt.finalSnapshotID != "" {
				body += "&FinalClusterSnapshotIdentifier=" + tt.finalSnapshotID
			}

			rec := postRedshiftForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"DeleteCluster skip=%s snapshotID=%q", tt.skipFinal, tt.finalSnapshotID)

			if tt.wantErr != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErr)
			}

			if tt.wantSnapshotAfter {
				snapRec := postRedshiftForm(t, h,
					"Action=DescribeClusterSnapshots&Version=2012-12-01&SnapshotIdentifier="+tt.finalSnapshotID)
				assert.Equal(t, http.StatusOK, snapRec.Code,
					"final snapshot %q should exist after deletion", tt.finalSnapshotID)
				assert.Contains(t, snapRec.Body.String(), tt.finalSnapshotID)
			}
		})
	}
}

// ----- DescribeClusters tag filtering -----

// TestDescribeClusters_TagFilter verifies that DescribeClusters supports
// filtering by TagKey and TagValue. Real AWS supports these filters.
func TestDescribeClusters_TagFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tagKey     string
		tagValue   string
		wantInBody []string
		wantAbsent []string
		wantCode   int
	}{
		{
			name:       "filter_by_tag_key_returns_matching_clusters",
			tagKey:     "env",
			wantInBody: []string{"tagged-cluster"},
			wantAbsent: []string{"untagged-cluster"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "filter_by_tag_key_and_value",
			tagKey:     "env",
			tagValue:   "prod",
			wantInBody: []string{"tagged-cluster"},
			wantAbsent: []string{"untagged-cluster"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "filter_by_nonexistent_tag_returns_empty",
			tagKey:     "does-not-exist",
			wantAbsent: []string{"tagged-cluster", "untagged-cluster"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "no_filter_returns_all",
			wantInBody: []string{"tagged-cluster", "untagged-cluster"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "filter_by_value_only",
			tagValue:   "prod",
			wantInBody: []string{"tagged-cluster"},
			wantAbsent: []string{"untagged-cluster"},
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=tagged-cluster")
			postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=untagged-cluster")
			postRedshiftForm(t, h,
				"Action=CreateTags&Version=2012-12-01&ResourceName=tagged-cluster&"+
					"Tags.Tag.1.Key=env&Tags.Tag.1.Value=prod")

			body := "Action=DescribeClusters&Version=2012-12-01"
			if tt.tagKey != "" {
				body += "&TagKey=" + tt.tagKey
			}
			if tt.tagValue != "" {
				body += "&TagValue=" + tt.tagValue
			}

			rec := postRedshiftForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantInBody {
				assert.Contains(t, rec.Body.String(), s,
					"expected %q in DescribeClusters response for TagKey=%q TagValue=%q",
					s, tt.tagKey, tt.tagValue)
			}

			for _, s := range tt.wantAbsent {
				assert.NotContains(t, rec.Body.String(), s,
					"expected %q absent in DescribeClusters response for TagKey=%q TagValue=%q",
					s, tt.tagKey, tt.tagValue)
			}
		})
	}
}

// TestDescribeClusters_Pagination verifies Marker/MaxRecords pagination.
func TestDescribeClusters_Pagination(t *testing.T) {
	t.Parallel()

	type pageTC struct {
		name       string
		query      string
		wantAbsent []string
		wantInBody []string
		wantCode   int
		wantMarker bool
	}

	tests := []pageTC{
		{
			name:       "no_params_returns_all",
			query:      "Action=DescribeClusters&Version=2012-12-01",
			wantCode:   http.StatusOK,
			wantInBody: []string{"alpha", "beta", "gamma"},
			wantMarker: false,
		},
		{
			name:       "max_records_1_returns_first",
			query:      "Action=DescribeClusters&Version=2012-12-01&MaxRecords=1",
			wantCode:   http.StatusOK,
			wantInBody: []string{"alpha", "Marker"},
			wantAbsent: []string{"beta", "gamma"},
			wantMarker: true,
		},
		{
			name:       "marker_advances_to_second_page",
			query:      "Action=DescribeClusters&Version=2012-12-01&MaxRecords=1&Marker=alpha",
			wantCode:   http.StatusOK,
			wantInBody: []string{"beta"},
			wantAbsent: []string{"alpha", "gamma"},
			wantMarker: true,
		},
		{
			name:       "marker_advances_to_last_page",
			query:      "Action=DescribeClusters&Version=2012-12-01&MaxRecords=1&Marker=beta",
			wantCode:   http.StatusOK,
			wantInBody: []string{"gamma"},
			wantAbsent: []string{"alpha", "beta"},
			wantMarker: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			for _, id := range []string{"alpha", "beta", "gamma"} {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier="+id)
			}

			rec := postRedshiftForm(t, h, tt.query)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantInBody {
				assert.Contains(t, rec.Body.String(), s, "expected %q in response", s)
			}

			for _, s := range tt.wantAbsent {
				assert.NotContains(t, rec.Body.String(), s, "unexpected %q in response", s)
			}
		})
	}
}

// ---- GetSupportedOperations ----

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantOps []string
	}{
		{
			name: "returns_all_supported_operations",
			wantOps: []string{
				"CreateCluster", "DeleteCluster", "DescribeClusters",
				"DescribeLoggingStatus", "DescribeTags", "CreateTags", "DeleteTags",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			ops := h.GetSupportedOperations()
			for _, op := range tt.wantOps {
				assert.Contains(t, ops, op)
			}
		})
	}
}

// ---- RouteMatcher edge cases ----

func TestRouteMatcher_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupReq func() *http.Request
		name     string
		want     bool
	}{
		{
			name: "valid_redshift_request",
			setupReq: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader("Action=CreateCluster&Version=2012-12-01"))
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

				return r
			},
			want: true,
		},
		{
			name: "GET_request_returns_false",
			setupReq: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/", nil)
			},
			want: false,
		},
		{
			name: "dashboard_path_returns_false",
			setupReq: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/dashboard/redshift",
					strings.NewReader("Action=CreateCluster&Version=2012-12-01"))
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

				return r
			},
			want: false,
		},
		{
			name: "non_form_content_type_returns_false",
			setupReq: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader(`{"Action":"CreateCluster"}`))
				r.Header.Set("Content-Type", "application/json")

				return r
			},
			want: false,
		},
		{
			name: "wrong_version_returns_false",
			setupReq: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader("Action=CreateCluster&Version=2010-05-08"))
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

				return r
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			e := echo.New()
			matcher := h.RouteMatcher()
			req := tt.setupReq()
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

// ---- ExtractOperation edge cases ----

func TestExtractOperation_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupReq func() *http.Request
		name     string
		want     string
	}{
		{
			name: "action_extracted",
			setupReq: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader("Action=CreateCluster&Version=2012-12-01"))
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

				return r
			},
			want: "CreateCluster",
		},
		{
			name: "no_action_returns_unknown",
			setupReq: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader("Version=2012-12-01"))
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

				return r
			},
			want: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			e := echo.New()
			req := tt.setupReq()
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

// ---- ExtractResource edge cases ----

func TestExtractResource_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupReq func() *http.Request
		name     string
		want     string
	}{
		{
			name: "cluster_id_extracted",
			setupReq: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader("Action=DescribeClusters&Version=2012-12-01&ClusterIdentifier=my-cluster"))
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

				return r
			},
			want: "my-cluster",
		},
		{
			name: "no_cluster_id_returns_empty",
			setupReq: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader("Action=DescribeClusters&Version=2012-12-01"))
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

				return r
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			e := echo.New()
			req := tt.setupReq()
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

// ---- handleOpError: InternalFailure for unknown error ----

func TestHandler_HandleOpError_InternalFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		action       string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "cluster_not_found",
			action:       "DeleteCluster",
			body:         "Action=DeleteCluster&Version=2012-12-01&ClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: "ClusterNotFound",
		},
		{
			name:         "cluster_already_exists",
			action:       "CreateCluster",
			body:         "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=dup-cluster",
			wantCode:     http.StatusBadRequest,
			wantContains: "ClusterAlreadyExists",
		},
		{
			name:         "invalid_parameter",
			action:       "CreateCluster",
			body:         "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.name == "cluster_already_exists" {
				// Create the cluster first so we get a duplicate error
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=dup-cluster")
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---- Handler: missing Action parameter ----

func TestHandler_MissingAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "missing_action",
			body:     "Version=2012-12-01",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "unknown_action",
			body:     "Action=UnknownOperation&Version=2012-12-01",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---- writeXMLResponse via successful handler invocations ----

func TestHandler_WriteXMLResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *redshift.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "describe_logging_status",
			body:         "Action=DescribeLoggingStatus&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: "DescribeLoggingStatusResponse",
		},
		{
			name:         "describe_tags_empty",
			body:         "Action=DescribeTags&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: "DescribeTagsResponse",
		},
		{
			name: "describe_tags_with_data",
			setup: func(h *redshift.Handler) {
				postRedshiftFormSetup(h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=tag-cluster")
				postRedshiftFormSetup(
					h,
					"Action=CreateTags&Version=2012-12-01&ResourceName=tag-cluster"+
						"&Tags.Tag.1.Key=env&Tags.Tag.1.Value=prod",
				)
			},
			body:         "Action=DescribeTags&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: "DescribeTagsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}
		})
	}
}

// postRedshiftFormSetup posts a form-encoded Redshift request for test setup purposes.
// The handler error is intentionally discarded since setup failures will be caught
// by subsequent assertions on the handler state.
func postRedshiftFormSetup(h *redshift.Handler, body string) {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	_ = h.Handler()(c) // setup helper, errors caught by subsequent test assertions
}

// ---- StorageBackend interface satisfaction ----

func TestStorageBackend_InterfaceSatisfied(t *testing.T) {
	t.Parallel()

	var _ redshift.StorageBackend = redshift.NewInMemoryBackend("000000000000", "us-east-1")
}

// ---- Handler.Reset ----

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=reset-c1")
	assert.Equal(t, 1, redshift.ClusterCount(b))

	h.Reset()

	assert.Equal(t, 0, redshift.ClusterCount(b))
}

// ---- GetSupportedOperations sorted ----

func TestHandler_GetSupportedOperations_Sorted(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i], "ops not sorted at index %d", i)
	}
}

// ---- HandlerOpsLen ----

func TestHandler_OpsLen(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	assert.Equal(t, len(h.GetSupportedOperations()), redshift.HandlerOpsLen(h))
}

// ---- CreateCluster returns NumberOfNodes and Port ----

func TestCreateCluster_ReturnsExpectedFields(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	rec := postRedshiftForm(t, h,
		"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=fields-cluster&NodeType=dc2.8xlarge")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "CreateClusterResponse")
	assert.Contains(t, body, "fields-cluster")
	assert.Contains(t, body, "dc2.8xlarge")
	// Port 5439 is default
	assert.Contains(t, body, "5439")
}

// ---- DescribeClusters: deep copy check ----

func TestDescribeClusters_DeepCopy(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateCluster("c1", "dc2.large", "dev", "admin")
	require.NoError(t, err)

	clusters, _, err := b.DescribeClusters("", "", 0)
	require.NoError(t, err)
	require.Len(t, clusters, 1)

	// Modifying the returned slice should not affect the backend
	clusters[0].ClusterIdentifier = "mutated"

	clusters2, _, err := b.DescribeClusters("", "", 0)
	require.NoError(t, err)
	assert.Equal(t, "c1", clusters2[0].ClusterIdentifier, "backend should not be mutated by caller")
}

// ---- Error code: InvalidParameterValue ----

func TestHandler_ErrorCode_InvalidParameterValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "create_cluster_missing_id",
			body: "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=",
		},
		{
			name: "cancel_resize_missing_id",
			body: "Action=CancelResize&Version=2012-12-01&ClusterIdentifier=",
		},
		{
			name: "accept_reserved_node_exchange_missing_id",
			body: "Action=AcceptReservedNodeExchange&Version=2012-12-01",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "InvalidParameterValue",
				"expected AWS-standard error code, not legacy RedshiftInvalidParameter")
		})
	}
}

// ---- ChaosServiceName and ChaosOperations ----

func TestHandler_ChaosFields(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	assert.Equal(t, "redshift", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
}
