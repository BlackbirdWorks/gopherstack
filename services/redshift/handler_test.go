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
			name: "describe_logging_status",
			setup: func(h *redshift.Handler) {
				postRedshiftFormSetup(h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=wxr-log-cluster")
			},
			body:         "Action=DescribeLoggingStatus&Version=2012-12-01&ClusterIdentifier=wxr-log-cluster",
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

func TestStorageBackend_InterfaceSatisfied(t *testing.T) {
	t.Parallel()

	var _ redshift.StorageBackend = redshift.NewInMemoryBackend("000000000000", "us-east-1")
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=reset-c1")
	assert.Equal(t, 1, redshift.ClusterCount(b))

	h.Reset()

	assert.Equal(t, 0, redshift.ClusterCount(b))
}

func TestHandler_GetSupportedOperations_Sorted(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i], "ops not sorted at index %d", i)
	}
}

func TestHandler_OpsLen(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	assert.Equal(t, len(h.GetSupportedOperations()), redshift.HandlerOpsLen(h))
}

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

func TestHandler_ChaosFields(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	assert.Equal(t, "redshift", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
}
