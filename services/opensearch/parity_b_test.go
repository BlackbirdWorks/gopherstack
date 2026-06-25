package opensearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParityB_AcceptInboundConnection_UnknownIDReturnsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		connectionID string
		wantCode     int
	}{
		{
			name:         "unknown connection id",
			connectionID: "nonexistent-conn-id",
			wantCode:     http.StatusNotFound,
		},
		{
			name:         "another unknown connection",
			connectionID: "ci-00000000",
			wantCode:     http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(
				t,
				h,
				http.MethodPut,
				"/2021-01-01/opensearch/cc/inboundConnection/"+tt.connectionID+"/accept",
				nil,
			)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}

func TestParityB_AssociatePackages_ValidatesPackageExistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		packageID  string
		domainName string
		wantCode   int
	}{
		{
			name:       "unknown package returns error",
			packageID:  "F-does-not-exist",
			domainName: "my-domain",
			wantCode:   http.StatusBadRequest,
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

			resp = doRequest(t, h, http.MethodPost, "/2021-01-01/packages/associateMultiple", map[string]any{
				"DomainName":  tt.domainName,
				"PackageList": []map[string]any{{"PackageID": tt.packageID}},
			})
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}

func TestParityB_DeleteDomain_CleansUpMaintenances(t *testing.T) {
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
				"/2021-01-01/opensearch/domain/"+tt.domainName+"/maintenance",
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
				"/2021-01-01/opensearch/domain/"+tt.domainName+"/maintenance",
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

func TestParityB_CancelDomainConfigChange_ReturnsLastChangeID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		domainName    string
		triggerUpdate bool
		wantEmptyIDs  bool
	}{
		{
			name:          "no pending change returns empty",
			domainName:    "cancel-domain-empty",
			triggerUpdate: false,
			wantEmptyIDs:  true,
		},
		{
			name:          "pending change returned and cleared",
			domainName:    "cancel-domain-pending",
			triggerUpdate: true,
			wantEmptyIDs:  false,
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

			if tt.triggerUpdate {
				resp = doRequest(
					t,
					h,
					http.MethodPut,
					"/2021-01-01/opensearch/domain/"+tt.domainName+"/config",
					map[string]any{
						"AccessPolicies": `{"Version":"2012-10-17"}`,
					},
				)
				resp.Body.Close()
				require.Equal(t, http.StatusOK, resp.StatusCode)
			}

			resp = doRequest(
				t,
				h,
				http.MethodPost,
				"/2021-01-01/opensearch/domain/"+tt.domainName+"/config/cancel",
				map[string]any{"DryRun": false},
			)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var got struct {
				CancelledChangeIDs []string `json:"CancelledChangeIds"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))

			if tt.wantEmptyIDs {
				assert.Empty(t, got.CancelledChangeIDs)
			} else {
				assert.NotEmpty(t, got.CancelledChangeIDs, "expected a change ID to be returned")
			}
		})
	}
}

func TestParityB_ListDomainNames_ReturnsBothDomains(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		domainNames []string
		wantCount   int
	}{
		{
			name:        "two domains both returned",
			domainNames: []string{"alpha-domain", "beta-domain"},
			wantCount:   2,
		},
		{
			name:        "single domain returned",
			domainNames: []string{"solo-domain"},
			wantCount:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			for _, dn := range tt.domainNames {
				resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
					"DomainName": dn,
				})
				resp.Body.Close()
				require.Equal(t, http.StatusOK, resp.StatusCode)
			}

			resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain", nil)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var got struct {
				DomainNames []struct {
					DomainName    string `json:"DomainName"`
					EngineVersion string `json:"EngineVersion"`
				} `json:"DomainNames"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
			assert.Len(t, got.DomainNames, tt.wantCount)

			returnedNames := make(map[string]bool, len(got.DomainNames))

			for _, e := range got.DomainNames {
				returnedNames[e.DomainName] = true
				assert.NotEmpty(t, e.EngineVersion, "expected non-empty EngineVersion for %s", e.DomainName)
			}

			for _, dn := range tt.domainNames {
				assert.True(t, returnedNames[dn], "expected domain %s in listing", dn)
			}
		})
	}
}
