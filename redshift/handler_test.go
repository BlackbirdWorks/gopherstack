package redshift_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/redshift"
)

func newRedshiftHandler() *redshift.Handler {
	return redshift.NewHandler(redshift.NewInMemoryBackend("000000000000", "us-east-1"), slog.Default())
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
		wantCode     int
		wantContains []string
	}{
		{
			name:         "success",
			body:         "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=test-cluster&NodeType=dc2.large&DBName=mydb&MasterUsername=admin",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateClusterResponse", "test-cluster"},
		},
		{
			name:     "empty_id",
			body:     "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=",
			wantCode: http.StatusBadRequest,
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
		name         string
		setup        func(t *testing.T, h *redshift.Handler)
		body         string
		wantCode     int
		wantContains []string
	}{
		{
			name: "list_all",
			setup: func(t *testing.T, h *redshift.Handler) {
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
