package integration_test

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func redshiftPost(t *testing.T, form url.Values) *http.Response {
	t.Helper()

	form.Set("Version", "2012-12-01")

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint,
		strings.NewReader(form.Encode()))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func redshiftReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_Redshift_CreateCluster(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := redshiftPost(t, url.Values{
		"Action":            {"CreateCluster"},
		"ClusterIdentifier": {"integ-cluster"},
		"NodeType":          {"dc2.large"},
		"DBName":            {"testdb"},
		"MasterUsername":    {"admin"},
	})
	body := redshiftReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "CreateClusterResponse")
	assert.Contains(t, body, "integ-cluster")
}

func TestIntegration_Redshift_DescribeClusters(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	redshiftPost(t, url.Values{
		"Action":            {"CreateCluster"},
		"ClusterIdentifier": {"describe-cluster"},
	})

	resp := redshiftPost(t, url.Values{
		"Action": {"DescribeClusters"},
	})
	body := redshiftReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "DescribeClustersResponse")
}

func TestIntegration_Redshift_DeleteCluster(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	redshiftPost(t, url.Values{
		"Action":            {"CreateCluster"},
		"ClusterIdentifier": {"delete-integ-cluster"},
	})

	resp := redshiftPost(t, url.Values{
		"Action":            {"DeleteCluster"},
		"ClusterIdentifier": {"delete-integ-cluster"},
	})
	body := redshiftReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "DeleteClusterResponse")
}
