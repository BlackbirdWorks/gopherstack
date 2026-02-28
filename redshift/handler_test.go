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

func TestRedshiftHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "CreateCluster",
			run: func(t *testing.T) {
				const createForm = "Action=CreateCluster&Version=2012-12-01" +
					"&ClusterIdentifier=test-cluster&NodeType=dc2.large&DBName=mydb&MasterUsername=admin"

				h := newRedshiftHandler()
				rec := postRedshiftForm(t, h, createForm)

				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "CreateClusterResponse")
				assert.Contains(t, rec.Body.String(), "test-cluster")
			},
		},
		{
			name: "CreateCluster_EmptyID",
			run: func(t *testing.T) {
				h := newRedshiftHandler()
				rec := postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=")

				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "DeleteCluster",
			run: func(t *testing.T) {
				h := newRedshiftHandler()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=del-cluster")

				rec := postRedshiftForm(t, h, "Action=DeleteCluster&Version=2012-12-01&ClusterIdentifier=del-cluster")
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "DeleteClusterResponse")
			},
		},
		{
			name: "DescribeClusters",
			run: func(t *testing.T) {
				h := newRedshiftHandler()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=desc-cluster")

				rec := postRedshiftForm(t, h, "Action=DescribeClusters&Version=2012-12-01")
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "DescribeClustersResponse")
				assert.Contains(t, rec.Body.String(), "desc-cluster")
			},
		},
		{
			name: "DescribeClusters_NotFound",
			run: func(t *testing.T) {
				h := newRedshiftHandler()
				rec := postRedshiftForm(t, h, "Action=DescribeClusters&Version=2012-12-01&ClusterIdentifier=nonexistent")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "InvalidAction",
			run: func(t *testing.T) {
				h := newRedshiftHandler()
				rec := postRedshiftForm(t, h, "Action=InvalidAction&Version=2012-12-01")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "MissingAction",
			run: func(t *testing.T) {
				h := newRedshiftHandler()
				rec := postRedshiftForm(t, h, "Version=2012-12-01")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
