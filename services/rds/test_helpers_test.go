package rds_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newAccuracyRDSHandler() *rds.Handler {
	b := rds.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return rds.NewHandler(b)
}

func doAccuracyRDS(t *testing.T, h *rds.Handler, vals url.Values) *httptest.ResponseRecorder {
	t.Helper()
	body := vals.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func mustCreateAccuracyRDSInstance(t *testing.T, h *rds.Handler, id string) {
	t.Helper()
	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {id},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
}

func newBatch2Backend() *rds.InMemoryBackend {
	return rds.NewInMemoryBackend("000000000000", "us-east-1")
}

func newBatch2Handler() *rds.Handler {
	return rds.NewHandler(newBatch2Backend())
}

func newBatch3Backend() *rds.InMemoryBackend {
	return rds.NewInMemoryBackend("123456789012", "us-east-1")
}

func newBatch3Handler() *rds.Handler {
	return rds.NewHandler(newBatch3Backend())
}

func newAccOps2Handler(t *testing.T) *rds.Handler {
	t.Helper()
	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	t.Cleanup(b.Close)

	return rds.NewHandler(b)
}

func doAccOps2(t *testing.T, h *rds.Handler, body string) *httptest.ResponseRecorder {
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

func mustCreateAccOps2Instance(t *testing.T, h *rds.Handler, id string) {
	t.Helper()
	rec := doAccOps2(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {id},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code, "setup: %s", rec.Body)
}

func mustCreateAccOps2Cluster(t *testing.T, h *rds.Handler, id string) {
	t.Helper()
	rec := doAccOps2(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {id},
		"Engine":              {"aurora-postgresql"},
		"MasterUsername":      {"admin"},
		"MasterUserPassword":  {"password123"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code, "setup cluster: %s", rec.Body)
}

// TestExportCountHelpers verifies the count helpers work correctly.
func TestExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")

	assert.Equal(t, 0, rds.InstanceCount(b))
	assert.Equal(t, 0, rds.ClusterCount(b))
	assert.Equal(t, 0, rds.SnapshotCount(b))
	assert.Equal(t, 0, rds.EventSubscriptionCount(b))
	assert.Equal(t, 0, rds.SecurityGroupCount(b))
	assert.Equal(t, 0, rds.BlueGreenDeploymentCount(b))

	b.AddInstanceInternal("i1", "mysql")
	b.AddClusterInternal("c1", "aurora")
	b.AddEventSubscriptionInternal("s1", "arn")
	b.AddBlueGreenDeploymentInternal("bgd-1", "arn:aws:rds:us-east-1:000:db:my-db")
	b.AddSecurityGroupInternal("sg1", "test sg")

	assert.Equal(t, 1, rds.InstanceCount(b))
	assert.Equal(t, 1, rds.ClusterCount(b))
	assert.Equal(t, 1, rds.EventSubscriptionCount(b))
	assert.Equal(t, 1, rds.BlueGreenDeploymentCount(b))
	assert.Equal(t, 1, rds.SecurityGroupCount(b))
}

// TestSeedHelpers verifies all seed helpers work correctly.
func TestSeedHelpers(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")

	cluster := b.AddClusterInternal("c1", "aurora-mysql")
	require.NotNil(t, cluster)
	assert.Equal(t, "c1", cluster.DBClusterIdentifier)

	inst := b.AddInstanceInternal("i1", "postgres")
	require.NotNil(t, inst)
	assert.Equal(t, "i1", inst.DBInstanceIdentifier)

	sub := b.AddEventSubscriptionInternal("s1", "arn:aws:sns:us-east-1:000:my-topic")
	require.NotNil(t, sub)
	assert.Equal(t, "s1", sub.SubscriptionName)

	bgd := b.AddBlueGreenDeploymentInternal("myname", "arn:aws:rds:us-east-1:000:db:my-db")
	require.NotNil(t, bgd)
	assert.Equal(t, "myname", bgd.BlueGreenDeploymentName)

	sg := b.AddSecurityGroupInternal("sg1", "my sg")
	require.NotNil(t, sg)
	assert.Equal(t, "sg1", sg.DBSecurityGroupName)
}

func newRDSHandler() *rds.Handler {
	return rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"))
}

func postRDSForm(t *testing.T, h *rds.Handler, body string) *httptest.ResponseRecorder {
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

func newTestBackend(t *testing.T) *rds.InMemoryBackend {
	t.Helper()
	b := rds.NewInMemoryBackend("123456789012", "us-east-1")
	t.Cleanup(b.Close)

	return b
}
