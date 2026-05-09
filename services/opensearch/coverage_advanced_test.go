package opensearch_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenSearch_UpgradeDomain(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createDomainAndGetARN(t, h, "upgradedom")

	// UpgradeDomain (POST to upgradeDomain path)
	resp := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/upgradeDomain", map[string]any{
			"DomainName":       "upgradedom",
			"TargetVersion":    "OpenSearch_2.11",
			"PerformCheckOnly": false,
		})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// GetUpgradeStatus (GET /domain/{name}/upgrades)
	resp = doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/upgradedom/upgrades", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// GetUpgradeHistory (GET /domain/{name}/upgradeHistory)
	resp = doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/upgradedom/upgradeHistory", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func TestOpenSearch_AutoTune(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createDomainAndGetARN(t, h, "autotunedom")

	// GetAutoTune (GET /domain/{name}/autoTunes)
	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/autotunedom/autoTunes", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func TestOpenSearch_InstanceTypeLimits(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/instanceTypeLimits/OpenSearch_2.11/r6g.large.search?domainName=testdom", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}
