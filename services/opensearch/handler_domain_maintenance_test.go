package opensearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDomainMaintenance_StartListGet(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createTestDomain(t, h, "maint-http-domain")

	// Start maintenance.
	sr := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/domain/maint-http-domain/domainMaintenance",
		map[string]any{"Action": "REBOOT_NODE", "NodeId": "node-0"})
	defer sr.Body.Close()
	require.Equal(t, http.StatusOK, sr.StatusCode)

	var sOut map[string]any
	require.NoError(t, json.NewDecoder(sr.Body).Decode(&sOut))
	maintID := sOut["MaintenanceId"].(string)
	assert.NotEmpty(t, maintID)

	// List maintenances for domain.
	lr := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/maint-http-domain/domainMaintenances", nil)
	defer lr.Body.Close()
	require.Equal(t, http.StatusOK, lr.StatusCode)

	var lOut map[string]any
	require.NoError(t, json.NewDecoder(lr.Body).Decode(&lOut))
	maintenances, ok := lOut["DomainMaintenances"].([]any)
	require.True(t, ok)
	assert.Len(t, maintenances, 1)

	// Get maintenance status by ID (maintenanceId is a query param, not a URL segment).
	gr := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/maint-http-domain/domainMaintenance?maintenanceId="+maintID, nil)
	defer gr.Body.Close()
	require.Equal(t, http.StatusOK, gr.StatusCode)

	var gOut map[string]any
	require.NoError(t, json.NewDecoder(gr.Body).Decode(&gOut))
	assert.Equal(t, maintID, gOut["MaintenanceId"])
}

func TestDeleteDomain_CleansUpMaintenances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domainName string
		action     string
	}{
		{
			name:       "maintenance records removed on delete",
			domainName: "maint-domain",
			action:     "REBOOT_NODE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
				"DomainName": tt.domainName,
			})
			resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			resp = doRequest(
				t,
				h,
				http.MethodPost,
				"/2021-01-01/opensearch/domain/"+tt.domainName+"/domainMaintenance",
				map[string]any{"Action": tt.action},
			)
			resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			resp = doRequest(t, h, http.MethodDelete, "/2021-01-01/opensearch/domain/"+tt.domainName, nil)
			resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			// Re-create to verify no orphaned maintenance records surface.
			resp = doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
				"DomainName": tt.domainName,
			})
			resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			resp = doRequest(
				t,
				h,
				http.MethodGet,
				"/2021-01-01/opensearch/domain/"+tt.domainName+"/domainMaintenances",
				nil,
			)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var got struct {
				DomainMaintenances []any `json:"DomainMaintenances"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
			assert.Empty(t, got.DomainMaintenances, "expected no orphaned maintenances after delete+recreate")
		})
	}
}
