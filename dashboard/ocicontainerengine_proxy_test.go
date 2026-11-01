package dashboard_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/ocicontainerengine"
)

const (
	okeComp = "ocid1.compartment.oc1..aaaa"
	okeBase = "/dashboard/api/v1/oci/containerengine"
)

func okeSeed(t *testing.T, e *echo.Echo, method, path, body string) []byte {
	t.Helper()

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	require.Less(t, rec.Code, http.StatusBadRequest, "%s: %s", path, rec.Body.String())

	return rec.Body.Bytes()
}

func newOKEDashboard(t *testing.T) (*dashboard.DashboardHandler, string, string) {
	t.Helper()

	b := ocicontainerengine.NewInMemoryBackend()
	b.SetTransition(0)
	t.Cleanup(b.Close)

	ops := ocicontainerengine.NewHandler(b)
	e := echo.New()
	e.Any("/*", ops.Trusted())

	okeSeed(t, e, http.MethodPost, "/20180222/clusters",
		`{"name":"prod","compartmentId":"`+okeComp+`","kubernetesVersion":"v1.34.2",`+
			`"vcnId":"ocid1.vcn.oc1.iad.vcna","endpointConfig":{"subnetId":"ocid1.subnet.oc1.iad.sub1"}}`)

	var clusters []struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(okeSeed(t, e, http.MethodGet,
		"/20180222/clusters?compartmentId="+okeComp, ""), &clusters))
	require.Len(t, clusters, 1)

	okeSeed(t, e, http.MethodPost, "/20180222/nodePools",
		`{"name":"workers","clusterId":"`+clusters[0].ID+`","compartmentId":"`+okeComp+`",`+
			`"nodeShape":"VM.Standard.E4.Flex","kubernetesVersion":"v1.34.2",`+
			`"nodeShapeConfig":{"ocpus":2,"memoryInGBs":16},`+
			`"nodeSourceDetails":{"sourceType":"IMAGE","imageId":"ocid1.image.oc1.iad.ol8"},`+
			`"nodeConfigDetails":{"size":2,"placementConfigs":[{"availabilityDomain":"Uocm:US-ASHBURN-AD-1",`+
			`"subnetId":"ocid1.subnet.oc1.iad.sub2"}]}}`)

	var pools []struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(okeSeed(t, e, http.MethodGet,
		"/20180222/nodePools?compartmentId="+okeComp, ""), &pools))
	require.Len(t, pools, 1)

	return dashboard.NewHandler(dashboard.Config{OCIContainerEngineOps: ops}), clusters[0].ID, pools[0].ID
}

func TestOCIContainerEngineProxy(t *testing.T) {
	t.Parallel()

	api := okeBase + "/api"

	tests := []struct {
		steps func(cluster, pool string) []gcpStep
		name  string
	}{
		{name: "read", steps: func(cluster, pool string) []gcpStep {
			return []gcpStep{
				{http.MethodGet, okeBase + "/compartments", "", okeComp, 200},
				{http.MethodGet, api + "/clusters?compartmentId=" + okeComp, "", `"name":"prod"`, 200},
				{http.MethodGet, api + "/clusters/" + cluster, "", `"kubernetesVersion":"v1.34.2"`, 200},
				{http.MethodGet, api + "/nodePools?compartmentId=" + okeComp + "&clusterId=" + cluster, "",
					`"name":"workers"`, 200},
				{http.MethodGet, api + "/nodePools/" + pool, "", `"nodes"`, 200},
				{http.MethodGet, api + "/workRequests?compartmentId=" + okeComp, "", "CLUSTER_CREATE", 200},
				{http.MethodGet, api + "/clusters/ocid1.cluster.oc1..nope", "", "NotAuthorizedOrNotFound", 404},
			}
		}},
		{name: "scale", steps: func(_, pool string) []gcpStep {
			return []gcpStep{
				{
					http.MethodPut,
					api + "/nodePools/" + pool,
					`{"nodeConfigDetails":{"size":4}}`,
					"",
					http.StatusAccepted,
				},
				{http.MethodGet, api + "/nodePools/" + pool, "", `"size":4`, 200},
				{http.MethodPut, api + "/nodePools/" + pool, `{"nodeConfigDetails":{"size":99999}}`, "size", 400},
			}
		}},
		{name: "scale forwards only the size", steps: func(_, pool string) []gcpStep {
			return []gcpStep{
				{
					http.MethodPut,
					api + "/nodePools/" + pool,
					`{"name":"renamed","nodeShape":"VM.Standard2.1","nodeConfigDetails":{"size":3}}`,
					"",
					http.StatusAccepted,
				},
				{http.MethodGet, api + "/nodePools/" + pool, "", `"name":"workers"`, 200},
				{http.MethodPut, api + "/nodePools/" + pool, `{"name":"renamed"}`, "nodeConfigDetails.size", 400},
				{http.MethodPut, api + "/nodePools/" + pool, `not json`, "nodeConfigDetails.size", 400},
			}
		}},
		{name: "delete", steps: func(cluster, _ string) []gcpStep {
			return []gcpStep{
				{http.MethodDelete, api + "/clusters/" + cluster, "", "", http.StatusAccepted},
				{http.MethodGet, api + "/workRequests?compartmentId=" + okeComp, "", "CLUSTER_DELETE", 200},
			}
		}},
		{name: "rejects", steps: func(cluster, pool string) []gcpStep {
			return []gcpStep{
				{http.MethodPost, api + "/clusters", `{}`, "", http.StatusMethodNotAllowed},
				{http.MethodPut, api + "/clusters/" + cluster, `{}`, "unsupported", 400},
				{http.MethodDelete, api + "/nodePools/" + pool, "", "unsupported", 400},
				{http.MethodDelete, api + "/nodePools/" + pool + "/node/n1", "", "unsupported", 400},
				{
					http.MethodPost,
					api + "/clusters/" + cluster + "/kubeconfig/content",
					`{}`,
					"",
					http.StatusMethodNotAllowed,
				},
				{http.MethodGet, api + "/clusters/" + cluster + "/kubeconfig/content", "", "unsupported", 400},
				{http.MethodGet, api + "/clusterOptions/all", "", "unsupported", 400},
				{http.MethodGet, api + "/clusters/%2e%2e", "", "unsupported", 400},
				{http.MethodGet, "/dashboard/api/v1/oci/containerengine/api/", "", "unsupported", 400},
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, cluster, pool := newOKEDashboard(t)
			runGCPSteps(t, h, tt.steps(cluster, pool))
		})
	}

	t.Run("credentials stay server side", func(t *testing.T) {
		t.Parallel()

		h, cluster, pool := newOKEDashboard(t)
		for _, p := range []string{
			api + "/clusters?compartmentId=" + okeComp, api + "/clusters/" + cluster, api + "/nodePools/" + pool,
		} {
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.NewContext(httptest.NewRequest(http.MethodGet, p, nil), rec)))

			for _, secret := range []string{"token", "caPem", "BEGIN CERTIFICATE", "kubeconfig"} {
				assert.NotContains(t, rec.Body.String(), secret, p)
			}
		}
	})

	t.Run("unserved", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, api+"/clusters?compartmentId="+okeComp, nil)
		require.NoError(t, dashboard.NewHandler(dashboard.Config{}).Handler()(echo.NewContext(req, rec)))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})
}
