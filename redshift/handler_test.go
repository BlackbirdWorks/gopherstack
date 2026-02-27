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

	h := newRedshiftHandler()
	rec := postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=test-cluster&NodeType=dc2.large&DBName=mydb&MasterUsername=admin")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateClusterResponse")
	assert.Contains(t, rec.Body.String(), "test-cluster")
}

func TestRedshiftHandler_CreateCluster_EmptyID(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	rec := postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
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

	h := newRedshiftHandler()
	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=desc-cluster")

	rec := postRedshiftForm(t, h, "Action=DescribeClusters&Version=2012-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeClustersResponse")
	assert.Contains(t, rec.Body.String(), "desc-cluster")
}

func TestRedshiftHandler_DescribeClusters_NotFound(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	rec := postRedshiftForm(t, h, "Action=DescribeClusters&Version=2012-12-01&ClusterIdentifier=nonexistent")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRedshiftHandler_InvalidAction(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	rec := postRedshiftForm(t, h, "Action=InvalidAction&Version=2012-12-01")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRedshiftHandler_MissingAction(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	rec := postRedshiftForm(t, h, "Version=2012-12-01")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
